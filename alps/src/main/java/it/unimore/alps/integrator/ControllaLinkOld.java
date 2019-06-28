package it.unimore.alps.integrator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.json.JSONException;

import com.github.cliftonlabs.json_simple.JsonObject;

import it.unimore.alps.sql.model.Link;
import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.Publication;

public class ControllaLinkOld {
	
	private static int numUrl200 = 0;
	private static int numUrl300 = 0;
	private static int numUrl400 = 0;
	private static int numUrl500 = 0;
	private static int numWrongUrl = 0;
	private static int numUrlFixable = 0;
	private static int numOtherError = 0;
	
	public static void main(String[] args) {
		
		CommandLine commandLine;

		Option DB = Option.builder("DB").hasArg().required(true).desc("DB. ").longOpt("DB").build();

		Options options = new Options();

		options.addOption(DB);

		CommandLineParser parser = new DefaultParser();

		String db = null;
		try {
			commandLine = parser.parse(options, args);

			if (commandLine.hasOption("DB")) {
				db = commandLine.getOptionValue("DB");
			} else {
				System.out.println("Source database is not provided. Use the DB option.");
				System.exit(0);
			}
			System.out.println("Database: " + db);

		} catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();
		
		// retrieve entities
		List<Organization> orgs = retrieveOrganizations(entitymanager);	
		
		int numTotalLink = 0;
		for (Organization org: orgs) {
			if (org.getLinks() != null) {
				numTotalLink += org.getLinks().size();
			}
		}
		System.out.println("Num. total urls: " + numTotalLink);
		
		correctWebsites(orgs, entitymanager);
		
		displayStats();
		
	}
	
	private static void displayStats() {
		System.out.println("STATS --------------------------");
		System.out.println("\tNum correct urls: " + numUrl200);
		System.out.println("\t\tNum wrong urls: " + numWrongUrl);
		System.out.println("\t\t300 error codes: " + numUrl300);
		System.out.println("\t\t400 error codes: " + numUrl400);
		System.out.println("\t\t500 error codes: " + numUrl500);
		System.out.println("\t\tother errors: " + numOtherError);
		System.out.println("\tNum fixed url: " + numUrlFixable);
		System.out.println("--------------------------------");
	}
	
	
	public static List<Organization> retrieveOrganizations(EntityManager entitymanager) {

		entitymanager.getTransaction().begin();
		Query query_org = entitymanager.createQuery("Select o FROM Organization o");
		List<Organization> orgs = query_org.getResultList();

		entitymanager.getTransaction().commit();
		System.out.println("Retrieved " + orgs.size() + " organizations");

		return orgs;
	}		
	
	public static void correctWebsites(List<Organization> orgs, EntityManager em) {
		
		em.getTransaction().begin();
		
		orgs.parallelStream().forEach(new Consumer<Organization>() {
			
			@Override
			public void accept(Organization org) {
				System.out.println("Current org: " + org.getLabel());
				CorrectUrl cUrl = new CorrectUrl();
				List<Link> links = org.getLinks();
				/*if (links.size()==0)  {
					System.out.println("No Url.");
				}*/
				for (Link link: links) {				
					
					String url = link.getUrl();
					//System.out.println("\tCurrent url: " + url);
					if(url!=null && !url.isEmpty() && !url.equalsIgnoreCase("null") && !url.equalsIgnoreCase("\"\"") && !url.equals(" ")) {				
											
						cUrl=request(url);
						
						if(cUrl.getCode()==404) {
							if (url.contains("/")) {
								//System.out.println("\t\tError code: " + cUrl.getCode());
								String urlModified = url.substring(0, url.lastIndexOf("/"));
								//System.out.println("\t\tTrying with new url: " + urlModified); 
								cUrl=request(urlModified);	
	
								if((cUrl.getCode()>=200 && cUrl.getCode()<300) || cUrl.getCode()==0) {
									//System.out.println("\t\tNew url accepted: " + cUrl.getLink());
									numUrlFixable += 1;
									link.setUrl(cUrl.getLink());
									em.merge(link);
								}
							}
						}
				   		if(cUrl.isRedirected()) {
				   			//System.out.println("\t\tRedirection to new url: " + cUrl.getLink());
				   			link.setUrl(cUrl.getLink());
				   			em.merge(link);
				   			numUrl300 += 1;
				   			numUrlFixable += 1;
				   		}
						
						if(cUrl.getCode()==200) {
							//System.out.println("Correct url");
							numUrl200 += 1;
						}
						if(cUrl.getCode()>=400 && cUrl.getCode()<500) {
							//System.out.println("\t\tError code (family 400): "+cUrl.getCode());
							numUrl400 += 1;
						}
						if(cUrl.getCode()>=500 && cUrl.getCode()<599) {
							//System.out.println("\t\tError code (family 500): "+cUrl.getCode());
							numUrl500 += 1;
						}
						if (cUrl.getCode() == 0) {
							numOtherError += 1;
							//System.out.println("Other error");
						}
					} else {
						//System.out.println("\t\tInvalid or null url");
					}
				}
			}
		});
		
		em.getTransaction().commit();
		
		
/*		for (Organization org: orgs) {
			System.out.println("Current org: " + org.getLabel());
			CorrectUrl cUrl = new CorrectUrl();
			List<Link> links = org.getLinks();
			if (links.size()==0)  {
				System.out.println("No Url.");
			}
			for (Link link: links) {				
				
				String url = link.getUrl();
				System.out.println("\tCurrent url: " + url);
				if(url!=null && !url.isEmpty() && !url.equalsIgnoreCase("null") && !url.equalsIgnoreCase("\"\"") && !url.equals(" ")) {
					
					urlCount += 1;					
					if ((urlCount % 100) == 0) {
						System.out.println(urlCount);
					}
										
					cUrl=request(url);
					
					if(cUrl.getCode()==404) {
						System.out.println("\t\tError code: " + cUrl.getCode());
						String urlModified = url.substring(0, url.lastIndexOf("/"));
						System.out.println("\t\tTrying with new url: " + urlModified); 
						cUrl=request(urlModified);	

						if((cUrl.getCode()>=200 && cUrl.getCode()<300) || cUrl.getCode()==0) {
							System.out.println("\t\tNew url accepted: " + cUrl.getLink());
							numUrlFixable += 1;
							//link.setUrl(cUrl.getLink());
							//em.merge(link);
						}
					}
			   		if(cUrl.isRedirected()) {
			   			System.out.println("\t\tRedirection to new url: " + cUrl.getLink());
			   			//link.setUrl(cUrl.getLink());
			   			//em.merge(link);
			   			numUrl300 += 1;
			   			numUrlFixable += 1;
			   		}
					
					if(cUrl.getCode()==200) {
						System.out.println("Correct url");
						numUrl200 += 1;
					}
					if(cUrl.getCode()>=400 && cUrl.getCode()<500) {
						System.out.println("\t\tError code (family 400): "+cUrl.getCode());
						numUrl400 += 1;
					}
					if(cUrl.getCode()>=500 && cUrl.getCode()<599) {
						System.out.println("\t\tError code (family 500): "+cUrl.getCode());
						numUrl500 += 1;
					}
					if (cUrl.getCode() == 0) {
						numOtherError += 1;
						System.out.println("Other error");
					}
				} else {
					System.out.println("\t\tInvalid or null url");
				}
			}
		} */		
	}
	
	
	public static CorrectUrl request(String link) {
		int code=0;
		URL url;
		CorrectUrl cUrl=new CorrectUrl();
		cUrl.setLink(link);
		try {
			url = new URL(link);
			HttpURLConnection connection = (HttpURLConnection)url.openConnection();
			connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36");	//Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11
			connection.setConnectTimeout(30000);
			connection.setReadTimeout(30000);
			connection.setRequestMethod("GET");
			connection.connect();
			if(connection.getResponseCode()>=300 && connection.getResponseCode()<400)
			{
				String newUrl = connection.getHeaderField("Location");
				cUrl.setRedirected(true);
				cUrl.setLink(newUrl);
			}
			//System.out.println(connection.getResponseMessage());
			cUrl.setCode(connection.getResponseCode()); 
		} catch (MalformedURLException e) {
			numWrongUrl += 1;
			//e.printStackTrace();
			link="http://"+link;
			cUrl=request(link);
			cUrl.setRedirected(true);
		} catch (ProtocolException e) {
			numWrongUrl += 1;
			//e.printStackTrace();
		} catch (IOException e) {
			numWrongUrl += 1;
			//e.printStackTrace();
		}catch (Exception e) { // UnknownHostException, SocketTimeoutException
			numWrongUrl += 1;
			//e.printStackTrace();
		}	
		return cUrl;
	}
}
