package it.unimore.alps.integrator;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Found on http://stackoverflow.com/questions/2993649/how-to-normalize-a-url-in-java
 */

/**
 * - Covert the scheme and host to lowercase (done by java.net.URL)
 * <p/>
 * - Normalize the path (done by java.net.URI)
 * <p/>
 * - Add the port number.
 * <p/>
 * - Remove the fragment (the part after the #).
 * <p/>
 * - Remove trailing slash.
 * <p/>
 * - Sort the query string params.
 * <p/>
 * - Remove some query string params like "utm_*" and "*session*".
 */
public class NormalizeURL {

    public static String normalizeForIdentification(final String taintedURL) {
        try {
            return normalize(taintedURL, NormalizeURL::buildIdentifierUrl);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static String normalize(final String taintedURL) throws MalformedURLException {
        return normalize(taintedURL, NormalizeURL::buildStandardUrl);
    }

    protected static String normalize(final String taintedURL, UrlBuilder builder) throws MalformedURLException {
        final URL url;
        try {
            url = new URI(taintedURL).normalize().toURL();
        } catch (URISyntaxException | IllegalArgumentException e) {
            throw new MalformedURLException(e.getMessage());
        }

        if (!(url.getProtocol().equals("http") || url.getProtocol().equals("https")))
            throw new MalformedURLException("Invalid protocol: " + url.getProtocol());

        if (url.getHost() == null || url.getHost().isEmpty()) {
            throw new MalformedURLException("Empty host on tainted url " + url);
        }

        final SortedMap<String, String> params = createParameterMap(url.getQuery());
        final int port = url.getPort();
        final String queryString;

        if (params != null) {
            queryString = "?" + params.entrySet().stream().map(it -> percentEncodeRfc3986(it.getKey())+"="+percentEncodeRfc3986(it.getValue())).collect(Collectors.joining("&"));
        } else {
            queryString = "";
        }

        boolean hasPort = port != -1
                && (port != 80 && url.getProtocol().equals("http") || port != 443
                && url.getProtocol().equals("https"));

        String path = url.getPath().replaceFirst("/$", "");

        String result = builder.buildUrl(url, path, queryString, hasPort, port);

        if (result.length() > 512)
            throw new MalformedURLException("URL is too long " + result.length());
        return result;
    }

    protected static String buildStandardUrl(URL url, String path, String queryString, boolean hasPort, long port) {
        return url.getProtocol() + "://" + url.getHost() + (hasPort ? ":" + port : "") + path + queryString;
    }

    protected static String buildIdentifierUrl(URL url, String path, String queryString, boolean hasPort, long port) {
        String[] pathAsArray = path.split("/");
        String page = pathAsArray[pathAsArray.length-1];
        if (page.startsWith("accueil.") || page.startsWith("index.")) {
            pathAsArray[pathAsArray.length-1] = "";
        }
        path = Arrays.stream(pathAsArray).collect(Collectors.joining("/")).replaceFirst("/$", "");
        String host = url.getHost();
        if (host.startsWith("www."))
            host = host.substring(4);
        return host + (hasPort ? ":" + port : "") + path + queryString;
    }

    /**
     * Takes a query string, separates the constituent name-value pairs, and stores them in a SortedMap ordered by
     * lexicographical order.
     *
     * @return Null if there is no query string.
     */
    private static SortedMap<String, String> createParameterMap(final String queryString) {
        if (queryString == null || queryString.isEmpty()) {
            return null;
        }

        final String[] pairs = queryString.split("&");
        final Map<String, String> params = new HashMap<>(pairs.length);

        for (final String pair : pairs) {
            if (pair.length() < 1) {
                continue;
            }

            String[] tokens = pair.split("=", 2);
            for (int j = 0; j < tokens.length; j++) {
                try {
                    tokens[j] = URLDecoder.decode(tokens[j], "UTF-8");
                } catch (UnsupportedEncodingException ex) {
                    throw new IllegalStateException("Should never happen", ex);
                }
            }
            switch (tokens.length) {
                case 1: {
                    if (pair.charAt(0) == '=') {
                        params.put("", tokens[0]);
                    } else {
                        if (isIgnoredParameter(tokens[0])) continue;
                        params.put(tokens[0], "");
                    }
                    break;
                }
                case 2: {
                    if (isIgnoredParameter(tokens[0])) continue;
                    params.put(tokens[0], tokens[1]);
                    break;
                }
            }
        }

        return new TreeMap<>(params);
    }

    private static boolean isIgnoredParameter(String key) {
        return key.startsWith("utm_") || key.contains("session");
    }

    /**
     * Percent-encode values according the RFC 3986. The built-in Java URLEncoder does not encode according to the RFC,
     * so we make the extra replacements.
     *
     * @param string Decoded string.
     * @return Encoded string per RFC 3986.
     */
    private static String percentEncodeRfc3986(final String string) {
        try {
            return URLEncoder.encode(string, "UTF-8").replace("+", "%20").replace("*", "%2A").replace("%7E", "~");
        } catch (UnsupportedEncodingException e) {
            return string;
        }
    }

    private interface UrlBuilder {
        String buildUrl(URL url, String path, String queryString, boolean hasPort, long port);
    }
}
