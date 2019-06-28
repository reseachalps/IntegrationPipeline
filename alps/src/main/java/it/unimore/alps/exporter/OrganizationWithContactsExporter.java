package it.unimore.alps.exporter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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

import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationExtraField;
import it.unimore.alps.utils.CSVFileWriter;

public class OrganizationWithContactsExporter {

	public static void main(String[] args) {	
		
		CommandLine commandLine;			
        Option countriesOption = Option.builder("countries")
        		.hasArg()
        		.required(true)
        		.desc("List of countries (splitted by ',') where organizations are located.")
        		.longOpt("countries")
        		.build();   
        
        
        Option DB = Option.builder("DB").hasArg().required(true).desc("DB. ").longOpt("DB").build();
        
        
        Option outputFolderOption = Option.builder("outputFolder")
        		.hasArg()
        		.required(true)
        		.desc("The output folder for the CSV file.")
        		.longOpt("outputFolder")
        		.build();   
        
        
        

        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addOption(countriesOption);
        options.addOption(DB);
        options.addOption(outputFolderOption);
        
        
        List<String> countryList = null;
        String db = null;
        String outputFolder = null;
                	
        try {
			commandLine = parser.parse(options, args);
		
			System.out.println("----------------------------");
			System.out.println("OPTIONS:");
			
	        if (commandLine.hasOption("countries")) {   
	        	String countries = commandLine.getOptionValue("countries");
	        	
	        	// check that the countries field is a valid list of countries 
	        	countryList = Arrays.asList(countries.split(","));
	        	if (countryList != null) {
	        		System.out.println("\tCountries: " + countries);
	        	} else {
	        		System.out.println("\tCountries parameter not valid.");
	        		System.exit(1);
	        	}
	        	
			} else {
				System.out.println("\tCountries not provided. Use the Countries option.");
	        	System.exit(1);
			}		        	        	      
	        
	        if(commandLine.hasOption("outputFolder")) {
	        	
	        	outputFolder = commandLine.getOptionValue("outputFolder");    
	        	System.out.println("\tOutput folder: " + outputFolder);
	        	
	        } else {
	        	System.out.println("\tOutput Folder is not provided. Use the DB option.");
	        	System.exit(1);
	        }
	        
	        if(commandLine.hasOption("DB")) {
	        	
	        	db =commandLine.getOptionValue("DB");
	        	System.out.println("\tDB: " + db);
    	
	        } else {
	        	System.out.println("\tDB name not provided. Use the outputFolder option.");
	        	System.exit(1);
	        }
	        
			
			System.out.println("----------------------------\n");
        
        } catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}                  
		
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();
		
		// retrieve all the organizations by country
		entitymanager.getTransaction().begin();			
		List<Organization> orgs = retrieveOrganizationsByCountries(entitymanager, countryList);
		entitymanager.getTransaction().commit();
		
		// group organizations by country
		Map<String, List<Organization>> orgsByCountryWithContacts = new HashMap<>();
		Map<Organization, List<String>> orgsContacts = new HashMap<>();
		int numOrgPerCountry = 100;
		for(Organization org: orgs) {
			
			String country = org.getCountry();
			
			// if less then numOrgPerCountry organizations have been collected for the current country
			if (!orgsByCountryWithContacts.containsKey(country) || (orgsByCountryWithContacts.containsKey(country) && orgsByCountryWithContacts.get(country).size() < numOrgPerCountry)) {
			
				// retrieve organization contacts
				List<String> orgContacts = getOrgContacts(org);
				
				// if organization has contacts
				if (orgContacts.size() > 0) {
					
					// save contacts associated to the current organization
					orgsContacts.put(org, orgContacts);
													
					// save organizations grouped by country
					List<Organization> orgsSingleCountry = null; 
					if (!orgsByCountryWithContacts.containsKey(country)) {
						orgsSingleCountry = new ArrayList<>();
						orgsSingleCountry.add(org);							
					} else {
						orgsSingleCountry = orgsByCountryWithContacts.get(country);
						orgsSingleCountry.add(org);				
					}
					orgsByCountryWithContacts.put(country, orgsSingleCountry);
				}
			}
			
		}
		
		// export organizations and associated contacts
		// use the information included in orgsContacts
		System.out.println("Exporting all the organizations...");
		List<List<String>> exportedOrganization = exportOrganizationsWithContacts(orgsContacts);
		CSVFileWriter writer = new CSVFileWriter(outputFolder+"organizations_with_contacts.csv");
        writer.write(exportedOrganization);
        System.out.println("Organizations exporting completed.");			
        
		entitymanager.close();
		emfactory.close();
		
	}	
	
	
	private static List<String> getOrgContacts(Organization org) {
			
		List<String> contacts = new ArrayList<>();
		
		List<OrganizationExtraField> oef = org.getOrganizationExtraFields();
		if (oef == null) {
			return contacts;
		} else {
			for (OrganizationExtraField of: oef) {
				if (of.getFieldKey().equals("crawled emails")) {
					contacts.add(of.getFieldValue());
				}
			}
		}
		
		return contacts;
	}
	
	public static List<Organization> retrieveOrganizationsByCountries(EntityManager entitymanager, List<String> countryList) {
		
		String countries = String.join(", ", countryList);
    	System.out.println("Starting retrieving organizations belonging to the countries: " + countries);
        Query queryOrg = entitymanager.createQuery("Select o FROM Organization o where o.country in :countryList");
        queryOrg.setParameter("countryList", countryList);
        List<Organization> orgs = queryOrg.getResultList();        
        System.out.println("Retrieved " + orgs.size() + " organizations");
        
        return orgs;
	}	
	
	private static void saveAddress(Organization org, List<String> orgData) {
		
		orgData.add(org.getAddress());
		orgData.add(org.getCity());
		orgData.add(org.getCountry());
		orgData.add(org.getCountryCode());
		orgData.add(org.getPostcode());
		if (org.getLat() > 0) {
			orgData.add(Float.toString(org.getLat()));
		} else {
			orgData.add(Float.toString(0));
		}
		if (org.getLon() > 0) {
			orgData.add(Float.toString(org.getLon()));
		} else {
			orgData.add(Float.toString(0));
		}
		orgData.add(org.getNutsLevel1());
		orgData.add(org.getNutsLevel2());
		orgData.add(org.getNutsLevel3());

	}
	
	
	public static List<List<String>> exportOrganizationsWithContacts(Map<Organization, List<String>> orgsContactsMap) {

		List<List<String>> orgsData = new ArrayList<List<String>>();
		
		List<String> header = new ArrayList<>(Arrays.asList("id", "label", "address", "city", "country", "countryCode", "postcode", "lat", "lon", "nuts1", "nuts2", "nuts3", "emails"));		
		
		orgsData.add(header);

		for (Organization org : orgsContactsMap.keySet()) {
			
			List<String> orgData = new ArrayList<String>();
			
			orgData.add(Integer.toString(org.getId()));
			orgData.add(org.getLabel());
			saveAddress(org, orgData);
									
			// organization contacts
			List<String> orgContacts = orgsContactsMap.get(org);		
			orgData.add(String.join(";", orgContacts));	

			orgsData.add(orgData);

		}
		
		return orgsData;

	}	

}
