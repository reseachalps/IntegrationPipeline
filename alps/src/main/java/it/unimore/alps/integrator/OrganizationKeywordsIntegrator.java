package it.unimore.alps.integrator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationExtraField;

public class OrganizationKeywordsIntegrator {
	
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
				System.out.println("Source database not provided. Use the DB option.");
				System.exit(1);
			}
			System.out.println("Database: " + db);						
	
		} catch (org.apache.commons.cli.ParseException e) {
			e.printStackTrace();
		}
	
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();			
						
		// retrieve organizations
		List<Organization> orgs = retrieveOrganizations(entitymanager);
		
		entitymanager.getTransaction().begin();
		
		// retrieve organization keywords from research alps endpoint (http://app.researchalps.eu/api/structures/<id>/keywords)
		int n=0;
		for (Organization org: orgs) {
			n+=1;
			if ((n%100) == 0) {
				System.out.println(n);
			}
			
			int id = org.getId();
			String orgUrl = "http://app.researchalps.eu/api/structures/" + id + "/keywords";
			URL url;
			try {
				url = new URL(orgUrl);
				
				HttpURLConnection connection = (HttpURLConnection) url.openConnection();
				connection.connect();
				int status = connection.getResponseCode();
				if (status == 200) {
								
					BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
					
					StringBuilder builder = new StringBuilder();
					for (String line = null; (line = reader.readLine()) != null;) {
					    builder.append(line).append("\n");
					}
					
					JSONParser jsonParser = new JSONParser();
					JSONArray finalResult = (JSONArray) jsonParser.parse(builder.toString());
					
					Iterator<JSONObject> it = finalResult.iterator();
					List<String> keywords = new ArrayList<>();
					while (it.hasNext()) {
						
						JSONObject keywordInfo = it.next();

						String keyVal = (String) keywordInfo.get("keyword");
			        	keywords.add(keyVal);
			        	
			        	
						/*for (Object key : keywordInfo.keySet()) {
					        String keyStr = (String)key;
					        if (keyStr.equals("keyword")) {
					        	String keyVal = (String) keywordInfo.get(keyStr);
					        	keywords.add(keyVal);
					        }					        
					    }*/											
					}
					
					if (keywords.size() > 0) {
						String keywordsString = String.join(";", keywords);
						
						List<OrganizationExtraField> extraFields = org.getOrganizationExtraFields();
						if (extraFields == null) {
							extraFields = new ArrayList<OrganizationExtraField>();
						}																		
						
						OrganizationExtraField orgExtraField = new OrganizationExtraField();
						orgExtraField.setFieldKey("keywords");
						orgExtraField.setVisibility(false);
						orgExtraField.setFieldValue(keywordsString);
						orgExtraField.setOrganization(org);
						extraFields.add(orgExtraField);
						
						org.setOrganizationExtraFields(extraFields);
						entitymanager.merge(org);
												
					}
					
					reader.close();
					connection.disconnect();
					
				} else {
					System.out.println("ERRORE");
				}
				
				
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			//break;
			
		}	
		
		entitymanager.getTransaction().commit();
		entitymanager.close();
		emfactory.close();
		
		
	}
	
	public static List<Organization> retrieveOrganizations(EntityManager entitymanager) {

		entitymanager.getTransaction().begin();
		Query queryOrg = entitymanager.createQuery("Select o FROM Organization o");
		List<Organization> orgs = queryOrg.getResultList();

		entitymanager.getTransaction().commit();
		System.out.println("Retrieved " + orgs.size() + " organizations");

		return orgs;
	}

}
