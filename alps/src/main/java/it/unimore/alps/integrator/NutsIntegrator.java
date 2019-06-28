package it.unimore.alps.integrator;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import it.unimore.alps.sql.model.Organization;

public class NutsIntegrator {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		/*
		 * The integration of NUTS codes requires the prior activation of a nodeJS server running on port 3000
		 * This nodeJS application reads from a predefined file (containing a mapping between geo-coordinates and NUTS codes)
		 *  and converts an input pair of geo-coordinates in a NUTS code
		 *  This server process can be activated by running: <path_to_nodejs>/bin/node nuts_server.js
		 */
		
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
		
		//List<Organization> orgs = retrieveOrganizationsWithLatLon(entitymanager);	
		List<Organization> orgs = retrieveOrganizations(entitymanager);
		
		entitymanager.getTransaction().begin();
				
		// loop over organizations and retrieve nuts codes
		if (orgs != null) {
			
			try {												
					
				for (Organization org: orgs) {	
					
					if (org.getLat() != null && org.getLat() != 0 && org.getLon() != null && org.getLon() != 0) {
						String url = "http://localhost:3000/get_nuts/";
						URL obj = new URL(url);
						HttpURLConnection con = (HttpURLConnection) obj.openConnection();					
						//add request header
						con.setRequestMethod("POST");									
						
						String urlParameters = "lat=" + org.getLat() + "&lon=" + org.getLon();
						
						// Send post request
						con.setDoOutput(true);
						DataOutputStream wr = new DataOutputStream(con.getOutputStream());
						wr.writeBytes(urlParameters);	
						wr.flush();
						wr.close();
									
				
						int responseCode = con.getResponseCode();
						System.out.println("\nSending 'POST' request to URL : " + url);
						System.out.println("Post parameters : " + urlParameters);
						System.out.println("Response Code : " + responseCode);
				
						BufferedReader in = new BufferedReader(
						        new InputStreamReader(con.getInputStream()));
						String inputLine;
						StringBuffer response = new StringBuffer();
				
						while ((inputLine = in.readLine()) != null) {
							response.append(inputLine);
						}
						in.close();
						
						//print result
						System.out.println(response.toString());
											
						try {
							JSONParser jsonParser = new JSONParser();
							JSONObject json = (JSONObject) jsonParser.parse(response.toString());
							String nuts1 = (String) json.get("alpgov_geo_nuts1");
							String nuts2 = (String) json.get("alpgov_geo_nuts2");
							String nuts3 = (String) json.get("alpgov_geo_nuts3");
							System.out.println("Nuts1: " + nuts1 + ", Nuts2: " + nuts2 + ", Nuts3: " + nuts3 );
							org.setNutsLevel1(nuts1);
							org.setNutsLevel2(nuts2);
							org.setNutsLevel3(nuts3);
							entitymanager.merge(org);
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					} else {
						
						// TODO: set the flag that indicates that this are fake data and don't derive from some of the input data sources
						
						if (org.getCountryCode().equals("SI")) {
							org.setNutsLevel1("SI0");
						}
						if (org.getCountryCode().equals("CH") ) {
							org.setNutsLevel1("CH0");
						}
					}
					
					
				}
				
				
			} catch (ProtocolException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		entitymanager.getTransaction().commit();
		entitymanager.close();
		emfactory.close();
		

	}
	
	public static List<Organization> retrieveOrganizationsWithLatLon(EntityManager entitymanager) {
		
		entitymanager.getTransaction().begin();
		System.out.println("Starting retrieving organizations with geo coordinates...");
	    Query queryOrg = entitymanager.createQuery("Select o FROM Organization o WHERE o.lat is not null and o.lon is not null");
	    List<Organization> orgs = queryOrg.getResultList();
	    entitymanager.getTransaction().commit();
	    System.out.println("Retrieved " + orgs.size() + " organizations.");
	    
	    return orgs;
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
