package it.unimore.alps.integrator;

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

import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.integrator.NutsConverter;

public class NutsCodesIntegrator {

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
		
		NutsConverter nutsCov = new NutsConverter();
		nutsCov.open();
				
		// loop over organizations and retrieve nuts codes
		int n=0;
		int integratedNuts = 0;
		int orgsNoLatLon = 0;
		if (orgs != null) {											
					
			for (Organization org: orgs) {	
				n+=1;
				if (org.getLat() != null && org.getLat() != 0 && org.getLon() != null && org.getLon() != 0) {
								
					integratedNuts += 1;
					
					nutsCov.setLat(org.getLat());
					nutsCov.setLon(org.getLon());										
					nutsCov.setNuts();
					//System.out.println(org.getLabel() + "-> nuts1: " + nutsCov.getNuts1() + ", nuts2: " + nutsCov.getNuts2()+ ", nuts3: " + nutsCov.getNuts3());
					
					org.setNutsLevel1(nutsCov.getNuts1());
					org.setNutsLevel2(nutsCov.getNuts2());
					org.setNutsLevel3(nutsCov.getNuts3());
					entitymanager.merge(org);
					
					if((n%1000) == 0) {
						System.out.println("Num. organizations integrated: " + n);
					}					
				} else {
					orgsNoLatLon +=1;
				}
			}							
		}
		
		nutsCov.close();
		
		entitymanager.getTransaction().commit();
		entitymanager.close();
		emfactory.close();
		
		System.out.println("Nuts codes integration completed successfully.");
		System.out.println("Integrated " + integratedNuts + " nuts codes.");
		System.out.println("Num. organizations without geo-coordinates: " + orgsNoLatLon);
		

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
