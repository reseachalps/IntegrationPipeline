package it.unimore.alps.sources.gerit;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.DataNode;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Crawler {

	public static void main(String[] args) {

		Integer id = 10045;

//		String url = "http://www.gerit.org/en/institutiondetail/" + id;
//
//		Crawler c = new Crawler();
//
//		String jsonString = c.getPageJsonInformation(url);
//
//		System.out.println("JSON:" + jsonString);
//
//		JSONObject main = new JSONObject(jsonString);
//
//		if (main.has("default")) {
//			System.out.println("Default");
//		}
//
//		List<Integer> ids = c.extractIds(jsonString);
//		System.out.println("Number of objects: " + ids.size());
//		ids.remove(id);
//
//		System.out.println("Number of objects: " + ids.size());

		Crawler c = new Crawler();

		c.downloadPageID(id);

	}

	public Crawler() {

	}

	public void downloadPageID(Integer id) {

		String url = "http://www.gerit.org/en/institutiondetail/" + id;

		Crawler c = new Crawler();

		String jsonString = c.getPageJsonInformation(url);
		if (jsonString != null) {

			System.out.println("JSON:" + jsonString);

			JSONObject main = new JSONObject(jsonString);

			if (main.has("default")) {
				System.out.println("Default");
			}

			List<Integer> ids = c.extractIds(jsonString);

			ids.remove(id);

			c.saveJsonToFile("jsonData/" + id + ".json", jsonString);

			for (Integer i : ids) {

				String url_ = "http://www.gerit.org/en/institutiondetail/" + i;
				System.out.println("DOWNLOAD PAGE ID: " + i + "\t\tURL: " + url_ + "  MAIN PAGE: " + id);
				String jsonString_ = c.getPageJsonInformation(url_);
				if (jsonString_ != null) {
					c.saveJsonToFile("jsonData/" + i + ".json", jsonString_);
				}
			}
		}

	}

	public List<Integer> extractIds(String json) {

		JSONObject main = new JSONObject(json);

		System.out.println("MAIN: " + main.keySet().toString());

		if (main.has("institutionDetail")) {
			JSONObject o1 = main.getJSONObject("institutionDetail");
			System.out.println("institution");
			System.out.println("institution: " + o1.keySet().toString());

			if (o1.has("institution")) {

				JSONObject o2 = o1.getJSONObject("institution");

				System.out.println("institution: " + o2.keySet().toString());

				String name = "";

				if (o2.has("name")) {
					JSONObject bildtitel = o2.getJSONObject("name");

					String name_en = "";
					if (bildtitel.get("en") != JSONObject.NULL) {
						name_en = bildtitel.getString("en");
					}
					String name_de = bildtitel.getString("de");

					if (!name_en.trim().equals("")) {
						name = name_en;
					} else {
						name = name_de;
					}

				}
				System.out.println("name: " + name);

				String type = null;

				if (o2.has("_einrichtungstypText")) {

					JSONObject _einrichtungstypText = o2.getJSONObject("_einrichtungstypText");

					String type_en = _einrichtungstypText.getString("en");
					String type_de = _einrichtungstypText.getString("de");
					if (!type_en.trim().equals("")) {
						type = type_en;
					} else if (!type_de.trim().equals("")) {
						type = type_de;
					}

				}
				System.out.println("type: " + type);

				String crossreflink_1 = null;

				if (o2.has("crossreflink_1")) {
					if (o2.get("crossreflink_1") != JSONObject.NULL) {
						String crossreflink = o2.getString("crossreflink_1");
						if (!crossreflink.trim().equals("")) {
							crossreflink_1 = crossreflink;
						}
					}
				}

				String wiki = null;
				if (o2.has("wikipedia")) {
					if (o2.get("wikipedia") != JSONObject.NULL) {
						JSONObject wikipedia = o2.getJSONObject("wikipedia");
						if (wikipedia.get("en") != JSONObject.NULL) {
							String wikipedia_en = wikipedia.getString("en");
							String wikipedia_de = wikipedia.getString("de");

							if (!wikipedia_en.trim().equals("")) {
								wiki = wikipedia_en;
							}
						}
					}

				}

				System.out.println("Wikipedia: " + wiki);

				String address = null;
				String city = null;
				String zipcode = null;
				String region = null;

				if (o2.has("anschrift")) {

					JSONObject anschrift = o2.getJSONObject("anschrift");

					city = anschrift.getString("ort");

					if (anschrift.get("plzvorort") != JSONObject.NULL) {
						zipcode = anschrift.getString("plzvorort");
					}
					if (anschrift.get("strasse") != JSONObject.NULL) {
						address = anschrift.getString("strasse");
					}

					if (anschrift.get("bundesland") != JSONObject.NULL) {
						region = anschrift.getString("bundesland");
					}

				}

				System.out.println("address: " + address);
				System.out.println("city: " + city);
				System.out.println("zipcode: " + zipcode);
				System.out.println("region: " + region);

				String grid = null;
				if (o2.has("grid")) {
					if (o2.get("grid") != JSONObject.NULL) {
						grid = o2.getString("grid");
					}

				}

				System.out.println("grid: " + grid);
				Double lon = null;
				Double lat = null;
				if (o2.has("geolocation")) {
					if (o2.get("geolocation") != JSONObject.NULL) {
						JSONObject geolocation = o2.getJSONObject("geolocation");

						lon = geolocation.getDouble("lon");
						lat = geolocation.getDouble("lat");
					}
				}

				System.out.println("lat: " + lat + "\tlon: " + lon);

				String url = null;
				if (o2.has("url")) {
					if (o2.get("url") != JSONObject.NULL) {
						url = o2.getString("url");
					}
				}
				System.out.println("url: " + url);

				Set<Integer> ids = new HashSet<>();
				if (o2.has("tree")) {
					JSONObject tree = o2.getJSONObject("tree");

					visitTree(tree, ids);

					System.out.println("Number of ids: " + ids.size());

				}

				List<Integer> results = new ArrayList<>();
				results.addAll(ids);

				return results;

			}

//			if (o1.has("institutionDetail")) {
//				System.out.println("INSTITUTIONALDETAIL");
//
//				JSONObject o2 = o1.getJSONObject("institutionDetail");
//
//				Set<String> keys = o2.keySet();
//
//				System.out.println("" + keys.toString());
//
//			}

		}

		return null;
	}

	void visitTree(JSONObject tree, Set<Integer> ids) {

		Integer id = tree.getInt("id");
		System.out.println("\t\tID: " + id);
		JSONObject name = tree.getJSONObject("name");

		String name_en = null;
		if (name.has("en")) {
			Object name_en_obj = name.get("en");
			if (name_en_obj != null) {
				if (!name_en_obj.equals("null")) {
					System.out.println(name_en_obj);
					// name_en = name.getString("en");
				}
			}
		}

		String name_de = name.getString("de");

		JSONArray children = tree.getJSONArray("children");
		ids.add(id);

		if (children.length() > 0) {

			for (int i = 0; i < children.length(); i++) {
				JSONObject child = children.getJSONObject(i);
				visitTree(child, ids);

			}
		}

	}

	public void saveJsonToFile(String file, String content) {
		Writer out;
		try {
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "UTF-8"));
			out.write(content);
			out.close();
		} catch (UnsupportedEncodingException | FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public String getPageJsonInformation(String url) {

		String result = null;

		Document doc;
		try {

			String script = null;
			// String url = "http://www.gerit.org/en/institutiondetail/10045";

			doc = Jsoup.connect(url).get();

			Elements scriptElements = doc.getElementsByTag("script");

			for (Element element : scriptElements) {
				for (DataNode node : element.dataNodes()) {
					System.out.println(node.getWholeData());

					String text = node.getWholeData();

					if (text.contains("window.__PRELOADED_STATE__")) {

						String pattern = "window.__PRELOADED_STATE__ =";

						int index = text.indexOf(pattern);

						if (index == -1) {
							throw new RuntimeException("Missing text!!!");
						}

						text = text.substring(index + pattern.length() + 1).trim();

						System.out.println("JSON: " + text);

						result = text;

					}

				}
				System.out.println("-------------------");
			}

		} catch (HttpStatusException e) {
			System.out.println("ERROR 404");

			// e.printStackTrace();
			return null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return result;

	}

}
