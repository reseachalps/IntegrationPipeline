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

import it.unimore.alps.sql.model.GeoCoordinate;
import it.unimore.alps.sql.model.Link;
import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.Publication;
import it.unimore.alps.sql.model.Website;

public class WebsiteIntegrator {
	
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
		Option WebsiteDB = Option.builder("WebsiteDB").hasArg().required(true).desc("WebsiteDB.").longOpt("WebsiteDB").build();
		Option disableInsertionInDB = Option.builder("disableDBInsertion").required(false).desc("Flag that disables the insertion of clean url in the indicated DB.").longOpt("disableDBInsertion").build();

		Options options = new Options();

		options.addOption(DB);
		options.addOption(WebsiteDB);
		options.addOption(disableInsertionInDB);

		CommandLineParser parser = new DefaultParser();

		String db = null;
		String websitedb = null;
		boolean disableDBInsertion = false;
		try {
			commandLine = parser.parse(options, args);

			if (commandLine.hasOption("DB")) {
				db = commandLine.getOptionValue("DB");
			} else {
				System.out.println("Source database not provided. Use the DB option.");
				System.exit(1);
			}
			System.out.println("Database: " + db);
			
			if (commandLine.hasOption("WebsiteDB")) {
				websitedb = commandLine.getOptionValue("WebsiteDB");
			} else {
				System.out.println("WebsiteDB database not provided. Use the WebsiteDB option.");
				System.exit(1);
			}
			System.out.println("WebsiteDB: " + websitedb);
			
			if (commandLine.hasOption("disableDBInsertion")) {
				disableDBInsertion = true;
			}
			System.out.println("Disable DB insertion: " + disableDBInsertion);

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
		
		// debug 
		//testSingleUrl(orgs, entitymanager, websitedb);
		//testOrgUrl(orgs, entitymanager, websitedb, 432140, true);
		//testOrgUrl(orgs, entitymanager, websitedb, 322328, true);
		//testOrgUrl(orgs, entitymanager, websitedb, 59393, true);
		//System.out.println("\n\n\n");
		//testOrgUrl(orgs, entitymanager, websitedb, 119735, true);		
		
		// deploy
		correctWebsites(orgs, entitymanager, websitedb, disableDBInsertion);
		
		//displayStats();		
		
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
	
	public static void correctWebsites(List<Organization> orgs, EntityManager em, String websitedb, boolean disableDBInsertion) {
		
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
					
					// try to retrieve clean website already stored in websitecorrect1 db
					EntityManagerFactory WebEmfactory = Persistence.createEntityManagerFactory(websitedb);
					EntityManager webem = WebEmfactory.createEntityManager();
					webem.getTransaction().begin();
				    Query query_web = webem.createQuery("Select w FROM Website w WHERE w.url=:url");
				    query_web.setParameter("url", url);
				    List<Website> websites = query_web.getResultList();
				    
				    if (websites != null && websites.size() > 0) { // if clean website already exists
				    	System.out.println("Website already existing.");
				    	Website website = websites.get(0);
				    	String correctUrl = website.getCorrectUrl();
				    	
				    	if (!disableDBInsertion) {
				    		link.setUrl(correctUrl);
				    		em.merge(link);
				    	}
				    	System.out.println("\tRetrieving url already existing.");
				    } else {
				    	
				    	System.out.println("\tRetrieving new url.");				    
				    	
						if(url!=null && !url.isEmpty() && !url.equalsIgnoreCase("null") && !url.equalsIgnoreCase("\"\"") && !url.equals(" ")) {				
								
							Website website = new Website();
							
							cUrl=request(url);
							
							if(cUrl.getCode()==404) {
								if (url.contains("/")) {
									//System.out.println("\t\tError code: " + cUrl.getCode());
									String urlModified = url.substring(0, url.lastIndexOf("/"));
									//System.out.println("\t\tTrying with new url: " + urlModified); 
									cUrl=request(urlModified);	
									
									System.out.println("\tRetrieving url already existing.");
		
									if((cUrl.getCode()>=200 && cUrl.getCode()<300) || cUrl.getCode()==0) {
										//System.out.println("\t\tNew url accepted: " + cUrl.getLink());
										numUrlFixable += 1;
										
										String newUrl = cUrl.getLink();
										if (!newUrl.startsWith("http")) {
											newUrl = "http://" + newUrl;
										}
										
										if (newUrl != null && !newUrl.equals("http://")) {
											
											if (!disableDBInsertion) {
												link.setUrl(newUrl);
												em.merge(link);	
											}
											cUrl.setCode(404);

											website.setUrl(url);
											website.setCorrectUrl(newUrl);
											webem.persist(website);
										}
										
									}
								}
							} else {
						   		if(cUrl.isRedirected() && cUrl.getCode() != 0) {
						   			//System.out.println("\t\tRedirection to new url: " + cUrl.getLink());
						   			
						   			numUrl300 += 1;
						   			numUrlFixable += 1;
						   			
						   			String newUrl = cUrl.getLink();
									if (!newUrl.startsWith("http")) {
										newUrl = "http://" + newUrl;
									}
						   			
									if (newUrl != null && !newUrl.equals("http://")) {
										
										if (!disableDBInsertion) {
											link.setUrl(newUrl);
											em.merge(link);
										}
										
										website.setUrl(url);
										website.setCorrectUrl(newUrl);
										webem.persist(website);
										
										if (url.equals("http://www.ing.unimore.it/") || url.equals("www.ing.unimore.it/") || url.equals("http://www.ing.unimore.it") || url.equals("www.ing.unimore.it")) {
											System.out.println("Dief new url after redirect: " + newUrl);
										}
									}
						   		} else {
						   			
						   			String newUrl = cUrl.getLink();
									if (!newUrl.startsWith("http")) {
										newUrl = "http://" + newUrl;
									}
							
							
									if(cUrl.getCode()==200) {
										//System.out.println("Correct url");
										website.setUrl(url);
										website.setCorrectUrl(newUrl);
										webem.persist(website);
										
										numUrl200 += 1;
									}
						   		}
							}
							/*if(cUrl.getCode()>=400 && cUrl.getCode()<500) {
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
							}*/
						} else {
							//System.out.println("\t\tInvalid or null url");
						}
						
						webem.getTransaction().commit();
				    }
				}
			}
		});
		
		em.getTransaction().commit();
		// BEGIN TEST SINGLE URL ------------------------------------------------------------------
		/*String url = "www.ing.unimore.it";
		//String url = "http://www.ing.unimore.it/";
		CorrectUrl cUrl = new CorrectUrl();
		// try to retrieve organization website
		EntityManagerFactory WebEmfactory = Persistence.createEntityManagerFactory(websitedb);
		EntityManager webem = WebEmfactory.createEntityManager();
		webem.getTransaction().begin();
	    Query query_web = webem.createQuery("Select w FROM Website w WHERE w.url=:url");
	    query_web.setParameter("url", url);
	    List<Website> websites = query_web.getResultList();
		if (websites != null && websites.size() > 0) { // if website already exists
	    	System.out.println("Website already existing.");
	    	Website website = websites.get(0);
	    	String correctUrl = website.getCorrectUrl();
	    	//link.setUrl(correctUrl);
   			//em.merge(link);
	    	System.out.println("\tRetrieving url already existing.");
	    } else {
	    	
	    	System.out.println("\tRetrieving new url.");	    
	    	
			if(url!=null && !url.isEmpty() && !url.equalsIgnoreCase("null") && !url.equalsIgnoreCase("\"\"") && !url.equals(" ")) {				
					
				Website website = new Website();
				
				cUrl=request(url);
				
				if(cUrl.getCode()==404) {
					if (url.contains("/")) {
						//System.out.println("\t\tError code: " + cUrl.getCode());
						String urlModified = url.substring(0, url.lastIndexOf("/"));
						//System.out.println("\t\tTrying with new url: " + urlModified); 
						cUrl=request(urlModified);	
						
						System.out.println("\tError 404. Trying to modifying the url.");

						if((cUrl.getCode()>=200 && cUrl.getCode()<300) || cUrl.getCode()==0) {
							//System.out.println("\t\tNew url accepted: " + cUrl.getLink());
							numUrlFixable += 1;
							
							String newUrl = cUrl.getLink();
							if (newUrl != null && !newUrl.equals("http://")) {
								System.out.println("\tFound valid url after 404.");
								//link.setUrl(newUrl);
								//em.merge(link);																														

								//website.setUrl(url);
								//website.setCorrectUrl(newUrl);
								//webem.persist(website);								
							}
							
						}
					}
				}
		   		if(cUrl.isRedirected()) {
		   			//System.out.println("\t\tRedirection to new url: " + cUrl.getLink());
		   			
		   			numUrl300 += 1;
		   			numUrlFixable += 1;
		   			
		   			String newUrl = cUrl.getLink();
					if (newUrl != null && !newUrl.equals("http://")) {
						//link.setUrl(newUrl);
						//em.merge(link);
						
						//website.setUrl(url);
						//website.setCorrectUrl(newUrl);
						//webem.persist(website);
						
						System.out.println("\tNew url after redirect: " + newUrl);
					}
		   		}
				
				if(cUrl.getCode()==200) {
					//System.out.println("Correct url");
					//website.setUrl(url);
					//website.setCorrectUrl(url);
					//webem.persist(website);
					//System.out.println("\tUrl was already valid.");
					
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
			
			webem.getTransaction().commit();
	    }
		
		
		em.getTransaction().commit();*/
		// END TEST SINGLE URL ------------------------------------------------------------------
		
		
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
	
	public static void testOrgUrl(List<Organization> orgs, EntityManager em, String websitedb, int orgId, boolean disableCache) {
		
		for (Organization org: orgs) {
			if (org.getId()==orgId) {
				
				List<Link> links = org.getLinks();
				if (links.size()==0)  {
					System.out.println("No Url.");
				}								
							
				for (Link link: links) {								
					String url = link.getUrl();				
					testSingleUrl(orgs, em, websitedb, url, disableCache);
				}
			}
		}	
		
	}
	
	
	public static void testSingleUrl(List<Organization> orgs, EntityManager em, String websitedb, String url, boolean disableCache) {
		
		em.getTransaction().begin();
				
		//String url = "www.ing.unimore.it";
		//String url = "http://www.ing.unimore.it/";
		
		CorrectUrl cUrl = new CorrectUrl();
				
		// try to retrieve clean website already stored in websitecorrect1 db
		EntityManagerFactory WebEmfactory = Persistence.createEntityManagerFactory(websitedb);
		EntityManager webem = WebEmfactory.createEntityManager();
		webem.getTransaction().begin();
	    Query query_web = webem.createQuery("Select w FROM Website w WHERE w.url=:url");
	    query_web.setParameter("url", url);
	    List<Website> websites = query_web.getResultList();
	    
		if (websites != null && websites.size() > 0 && !disableCache) { // if clean website already exists
	    	System.out.println("Clean website already exists.");
	    	Website website = websites.get(0);
	    	String correctUrl = website.getCorrectUrl();
	    	//link.setUrl(correctUrl);
   			//em.merge(link);
	    	System.out.println("\tRetrieved url already existing: " + correctUrl);
	    } else {
	    	
	    	System.out.println("\tRetrieving new clean url.");	    
	    	
			if(url!=null && !url.isEmpty() && !url.equalsIgnoreCase("null") && !url.equalsIgnoreCase("\"\"") && !url.equals(" ")) {				
					
				Website website = new Website();
				
				cUrl=request(url);
				
				if(cUrl.getCode()==404) {
					if (url.contains("/")) {
						//System.out.println("\t\tError code: " + cUrl.getCode());
						String urlModified = url.substring(0, url.lastIndexOf("/"));
						System.out.println("\t\tTrying with new url: " + urlModified); 
						cUrl=request(urlModified);	
						
						System.out.println("\tError 404. Trying to modify the url.");

						if((cUrl.getCode()>=200 && cUrl.getCode()<300) || cUrl.getCode()==0) {
							//System.out.println("\t\tNew url accepted: " + cUrl.getLink());
							numUrlFixable += 1;
							
							String newUrl = cUrl.getLink();
							if (newUrl != null && !newUrl.equals("http://")) {
								System.out.println("\tFound valid url after url change: " + newUrl);
								//link.setUrl(newUrl);
								//em.merge(link);																														
								System.out.println("Inserimento in DB 404");
								cUrl.setCode(404);
								//website.setUrl(url);
								//website.setCorrectUrl(newUrl);
								//webem.persist(website);								
							}
							
						} else {
							System.out.println("\tClean website not found after the url change");
						}
					}
				}
		   		if(cUrl.isRedirected() && cUrl.getCode() != 0) {
		   			System.out.println("\t\tRedirection to new url: " + cUrl.getLink());
		   			System.out.println("HTTP code: " + cUrl.getCode());
		   			
		   			numUrl300 += 1;
		   			numUrlFixable += 1;
		   			
		   			String newUrl = cUrl.getLink();
					if (newUrl != null && !newUrl.equals("http://")) {
						//link.setUrl(newUrl);
						//em.merge(link);
						System.out.println("Inserimento in DB");
						//website.setUrl(url);
						//website.setCorrectUrl(newUrl);
						//webem.persist(website);
						
						System.out.println("\tNew url after redirect: " + newUrl);
					} else {
						System.out.println("\tClean website not found after redirect");
					}
		   		}
				
				if(cUrl.getCode()==200) {
					System.out.println("Website already clean");
					//website.setUrl(url);
					//website.setCorrectUrl(url);
					//webem.persist(website);
					System.out.println("Inserimento in DB");
					//System.out.println("\tUrl was already valid.");
					
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
				System.out.println("\t\tInvalid or null url");
			}
			
			webem.getTransaction().commit();
	    }
		
		
		em.getTransaction().commit();
					
	}
	
	
	public static CorrectUrl request(String link) {
		System.out.println("Fetching url: " + link);
		int code=0;
		URL url;
		CorrectUrl cUrl=new CorrectUrl();
		cUrl.setLink(link);
		HttpURLConnection connection = null;
		try {
			url = new URL(link);
			connection = (HttpURLConnection)url.openConnection();
			connection.setRequestProperty("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36");//"Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:56.0) Gecko/20100101 Firefox/56.0");//"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.119 Safari/537.36");	//Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11
			connection.setConnectTimeout(30000);
			connection.setInstanceFollowRedirects(false);
			connection.setReadTimeout(30000);
			connection.setRequestMethod("GET");
			connection.connect();
			int status = connection.getResponseCode();
			System.out.println("Status: " + status);
			
			/*if (status != HttpURLConnection.HTTP_OK) {
				if (status == HttpURLConnection.HTTP_MOVED_TEMP
					|| status == HttpURLConnection.HTTP_MOVED_PERM
						|| status == HttpURLConnection.HTTP_SEE_OTHER)
				System.out.println("Redirect");
			}*/
			
			if(connection.getResponseCode()>=300 && connection.getResponseCode()<400)
			{
				String newUrl = connection.getHeaderField("Location");
				if (newUrl.startsWith("/")) {
					newUrl = url.getProtocol() + "://" + url.getHost() + newUrl;
	            }
				
				cUrl.setRedirected(true);
				cUrl.setLink(newUrl);
				System.out.println("Redirect to new url: " + newUrl);
			}
			//System.out.println(connection.getResponseMessage());
			cUrl.setCode(connection.getResponseCode()); 
			System.out.println("No error.");
		} catch (MalformedURLException e) {
			System.out.println("MalformedURLException");
			numWrongUrl += 1;
			//e.printStackTrace();
			link="http://"+link;
			cUrl=request(link);
			cUrl.setRedirected(true);
		} catch (ProtocolException e) {
			numWrongUrl += 1;
			e.printStackTrace();
		} catch (IOException e) {
			numWrongUrl += 1;
			e.printStackTrace();
		}catch (Exception e) { // UnknownHostException, SocketTimeoutException
			numWrongUrl += 1;
			e.printStackTrace();
		}	
		return cUrl;
	}
}
