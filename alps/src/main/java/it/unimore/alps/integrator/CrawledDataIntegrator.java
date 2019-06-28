package it.unimore.alps.integrator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.collections4.map.HashedMap;
import org.bson.Document;
import org.json.JSONException;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.core.DockerClientBuilder;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

import static com.mongodb.client.model.Filters.eq;

import it.unimore.alps.sql.model.Link;
import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationExtraField;

public class CrawledDataIntegrator {
	
public static void main(String[] args) throws JSONException {
	
	
		// In order to execute this integration, this code has to be runned in a docker container that have
		// access to the docker container of research alps 1 which contains the mongodb instance
		// the command to create this docker container is:
		// sudo docker run -d -it -v /home:/home --net researchalps --entrypoint="/bin/sh" --name matteop-script java
		// this command creates a docker container named matteop-script with has available the last version of java and a stardard bash shell
		// inside this docker it is possible to execute this java code normally
		// the researchalps network (indicated in the previous command) connect rsa1 and rsa2 via a bridge host
		
		// in order to access to this container, that is already existing in the machine, it is need to execute this command:
		// sudo docker exec -it matteop-script bash
	
		// in order to be able to access to the mysql db there are two possibilities:
		// 1) connecting to the bridge host (172.17.0.1) port 3306
		// 2) in phase of docker container creation, execute the command "sudo docker network connect bridge matteop-script"
	
	
		
		// read parameter for the db

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
				System.out.println("Source database not provided. Use the DB option.");
				System.exit(1);
			}
			System.out.println("Database: " + db);			

		} catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();			
		
		/**** Connect to MongoDB ****/
		MongoCollection<Document> collection = null;
		try {
			
			// ip dei containers docker presenti su RSA2: cat /var/run/docker.ips
			// curl 172.18.0.5:8080
			//MongoClient mongo = new MongoClient("172.19.0.7", 27017);
			//MongoClient mongo = new MongoClient("127.0.0.1", 9000);
			//MongoClient mongo = new MongoClient("54.36.60.130", 9000);		
			MongoClient mongo = new MongoClient("scanr-mongo", 27017);
			
			/*MongoCursor<String> dbsCursor = mongo.listDatabaseNames().iterator();
			while(dbsCursor.hasNext()) {
			    System.out.println(dbsCursor.next());
			}*/

			/**** Get database ****/
			// if database doesn't exists, MongoDB will create it for you
			MongoDatabase database = mongo.getDatabase("researchalps");
			System.out.println("Successfully connected to database");
			
			/**** Get collection ****/
			collection = database.getCollection("website"); 
			System.out.println("Collection user selected successfully: " + collection.count() + " elements");
			
			
		} catch (Exception e) {
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
		}
		
		int notFound = 0;
		int found = 0;
		int totalNum = 0;
		
		// loop over organizations and integrate crawled information
		if (collection != null) {
			List<Organization> orgs = retrieveOrganizations(entitymanager);
			totalNum = orgs.size();
			
			entitymanager.getTransaction().begin();
			if (orgs != null) {
				for(Organization org: orgs) {
					List<Link> links = org.getLinks();
					if (links != null) {
						Map<String, Integer> visitedUrls = new HashMap<>();
						for(Link link: links) {
							String url = link.getUrl();
							try {
								String normalizedUrl = NormalizeURL.normalizeForIdentification(url);
								System.out.println("Url: " + url + ", Normalized url: " + normalizedUrl);
								
								if (!visitedUrls.containsKey(normalizedUrl)) {
									visitedUrls.put(normalizedUrl, 1);
									
									Document document = collection.find(eq("_id", normalizedUrl)).first();
									if (document == null) {
									    //Document does not exist
										System.out.println("Document does not exist");
										notFound += 1;
									} else {
									    //We found the document
										System.out.println("We found the document");
										//System.out.println(document);
										
										List<OrganizationExtraField> oefNewList = retrieveRelevantInfoFromDocument(document, org);
																			
										List<OrganizationExtraField> oefOldList = org.getOrganizationExtraFields();
										if (oefOldList != null) {
											for(OrganizationExtraField oef : oefNewList) {
												oefOldList.add(oef);
											}
										} else {
											oefOldList = oefNewList;
										}
										org.setOrganizationExtraFields(oefOldList);
										entitymanager.merge(org);
										
										
										found += 1;
									}
								}
								
								//System.exit(-1);
								
								
					        } catch (IllegalArgumentException e) {
					        	System.out.println("Url " + url + " is malformed.");
					        }
							
						}
					}
				}
			}
			
			entitymanager.getTransaction().commit();
		}
		
		System.out.println("Found " + found + "/" + totalNum + " organizations in the mongoDB server");
		System.out.println("Not found " + notFound + "/" + totalNum + " organizations in the mongoDB server");
		
		
		// TODO: connect to MONGODB that executes inside the container 7ee05fd5d1a0 in ResearchAlps1 server 
		// command line: docker exec -it 7ee05fd5d1a0 mongo researchalps -> collection website
		/*String dockerName = "/scanr-mongo-dev";
		DockerClient dockerClient = DockerClientBuilder.getInstance().build();
		List<Container> containers = dockerClient.listContainersCmd().exec();
		System.out.println("Docker id...");
		Container container = null;
		for(Container cont: containers) {
			String containerName = cont.getNames()[0]; 
			if (containerName.equals(dockerName)) {
				container = cont;
			}
		}
		System.out.println("Docker id.");*/
								
		
	}

	public static List<OrganizationExtraField> retrieveRelevantInfoFromDocument(Document document, Organization org) {
		
		List<OrganizationExtraField> oefList = new ArrayList<>();
		
		String fieldPrefix = "crawled";
		
		// retrieve information about web links
		ArrayList<String> webLinks = new ArrayList<String>(Arrays.asList("rss"));
		for (String webLink: webLinks) {			// loop over all fields that store information about web links			
			if ( document.containsKey(webLink) ) {	// if field exists in current document
				List<Document> webLinksDocuments = (List<Document>) document.get(webLink);
				if (webLinksDocuments != null) {
					
					List<String> webLinkDocumentsList = new ArrayList<>(); 
					for(Document webLinkDocuments: webLinksDocuments) { 	// loop over all sub-documents included in the current field
						String webLinkUrl = (String) webLinkDocuments.get("url");
						if (webLinkUrl != null) {
							webLinkDocumentsList.add(webLinkUrl);			// save the url
						}												
					}
					
					if (webLinkDocumentsList.size() > 0) {					// if the document contains information about web links save this information in a single extra field item
						OrganizationExtraField oef = new OrganizationExtraField();
						String webLinkList = String.join(",", webLinkDocumentsList );
						oef.setFieldKey(fieldPrefix + " " + webLink);
						oef.setFieldValue(webLinkList);
						oef.setOrganization(org);
						oef.setVisibility(false);
						oefList.add(oef);
						System.out.println(webLink + ": " + webLinkList);
					}										
				}
			}
		}
		
						
		ArrayList<String> accounts = new ArrayList<String>( 
	            Arrays.asList("facebook", "linkedIn", "viadeo", "youtube", "twitter", "googlePlus", "dailymotion", "vimeo", "instagram"));
		
		for (String account: accounts) {
			if ( document.containsKey(account) ) {
				List<Document> socialDocuments = (List<Document>) document.get(account);
				if (socialDocuments != null) {
					
					List<String> socialDocumentsList = new ArrayList<>(); 
					for(Document socialDocument: socialDocuments) { 
						String socialAccount = (String) socialDocument.get("account");
						if (socialAccount != null) {
							socialDocumentsList.add(socialAccount);
						}
					}
					
					if (socialDocumentsList.size() > 0) {
						OrganizationExtraField oef = new OrganizationExtraField();
						String accountString = fieldPrefix + " " + account + " account";
						String accountList = String.join(",", socialDocumentsList ); 
						oef.setFieldKey(accountString);
						oef.setFieldValue(accountList);
						oef.setOrganization(org);
						oef.setVisibility(false);
						oefList.add(oef);
						System.out.println(accountString + ": " + accountList);
					}
											
				}
			}
		}
		
		ArrayList<String> contacts = new ArrayList<String>(Arrays.asList("emails", "phones", "faxes", "contactForms"));	
		for (String contact: contacts) {	
			if ( document.containsKey(contact) ) {
				List<String> singleContacts = (List<String>) document.get(contact);
				if (singleContacts != null) {
					
					if (singleContacts.size() > 0) {
						String contactsString = String.join(",", singleContacts );
						OrganizationExtraField oef = new OrganizationExtraField(); 
						oef.setFieldKey(fieldPrefix + " " + contact);
						oef.setFieldValue(contactsString);
						oef.setOrganization(org);
						oef.setVisibility(false);
						oefList.add(oef);
						System.out.println(contact + ": " + contactsString);
					}
					
				}
			}
		}
		

		
		
		
		
		
		return oefList;
		
	}

	public static List<Organization> retrieveOrganizations(EntityManager entitymanager) {
		
		entitymanager.getTransaction().begin();
		System.out.println("Starting retrieving organizations...");
		Query queryOrg = entitymanager.createQuery("Select o FROM Organization o");
	    List<Organization> orgs = queryOrg.getResultList();
	    entitymanager.getTransaction().commit();
	    System.out.println("Retrieved " + orgs.size() + " organizations.");
	    
	    return orgs;
	}
	

}
