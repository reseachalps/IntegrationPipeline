package it.unimore.alps.sources.foen;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

import com.github.cliftonlabs.json_simple.JsonArray;
import com.opencsv.CSVReader;

import it.unimore.alps.sources.openaire.OpenAireDataIntegrator;
import it.unimore.alps.sql.model.Leader;
import it.unimore.alps.sql.model.Link;
import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationActivity;
import it.unimore.alps.sql.model.OrganizationExtraField;
import it.unimore.alps.sql.model.OrganizationIdentifier;
import it.unimore.alps.sql.model.OrganizationType;
import it.unimore.alps.sql.model.Person;
import it.unimore.alps.sql.model.PersonExtraField;
import it.unimore.alps.sql.model.Source;

public class FoenImporter {

	private Reader dataReader;													// organization file reader
	private String sourceName = "Federal Office for the Environment - FOEN";	// data source name
	private String sourceUrl = "https://www.bafu.admin.ch/bafu/en/home.html";	// data source url
	private String sourceRevisionDate = "16-10-2018";							// data source date

	public static void main(String[] args) {

		// BEGIN: INPUT PARAMETERS ------------------------------------------------------
		CommandLine commandLine;
		// organization file
		Option dataOption = Option.builder("data").hasArg().required(true)
				.desc("The file that contains FOEN data. ").longOpt("data").build();
		// database where to import data
		Option DB = Option.builder("DB").hasArg().required(true).desc("DB. ").longOpt("DB").build();

		Options options = new Options();
		CommandLineParser parser = new DefaultParser();

		options.addOption(dataOption);
		options.addOption(DB);

		String data = null;
		String db = null;
		try {
			commandLine = parser.parse(options, args);

			System.out.println("----------------------------");
			System.out.println("OPTIONS:");
			
			if(commandLine.hasOption("DB")) {       	
	        	db =commandLine.getOptionValue("DB");
	        	System.out.println("DB name: " + db);
	        } else {
	        	System.out.println("\tDB name not provided. Use the DB option.");
	        	System.exit(1);
	        }
			
			if (commandLine.hasOption("data")) {
				data = commandLine.getOptionValue("data");
				System.out.println("\tFOEN main file: " + data);
			} else {
				System.out.println("\tFOEN main file not provided. Use the data option.");
				System.exit(1);
			}			

			System.out.println("----------------------------\n");

		} catch (org.apache.commons.cli.ParseException e) {
			e.printStackTrace();
		}
		// END: INPUT PARAMETERS --------------------------------------------------------

		// import data
		System.out.println("Starting importing FOEN data...");
		FoenImporter foenImporter = new FoenImporter(data);
		foenImporter.importData(db);

	}		 
	
	public FoenImporter(String data) {
		
		// load input CSV file in memory
		try {
			this.dataReader = new FileReader(data);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

	}

	private Source setSource() {
		
		// save data source information
		Source source = new Source();
		source.setLabel(sourceName);
		source.setUrl(this.sourceUrl);
		DateFormat df = new SimpleDateFormat("dd-MM-yyyy");

		Date sourceDate;
		try {
			sourceDate = df.parse(sourceRevisionDate);
			source.setRevisionDate(sourceDate);
		} catch (java.text.ParseException e) {
			e.printStackTrace();
		}

		return source;
	}

	private Organization setOrganizationFields(JSONObject data, List<Source> sources) {

		// save organization information into Organization object
		Organization org = new Organization();

		JSONObject organisation = (JSONObject) data.get("organisation");
		if (organisation != null) {
			
			// foundation always null
			
			JSONObject contact = (JSONObject) organisation.get("contact");
			if (contact != null) {
				// internet always []
				// phone always []
				JSONArray address = (JSONArray) contact.get("address");
				if (address != null) {
					Iterator<JSONObject> it = address.iterator();
					while (it.hasNext()) {												

						JSONObject addressData = it.next();
						
						JSONObject postalAddress = (JSONObject) addressData.get("postalAddress");
						if (postalAddress != null) {
							JSONObject addressInformation = (JSONObject) postalAddress.get("addressInformation");
							
							if (addressInformation != null) {
								String street = (String) addressInformation.get("street");								
								String houseNumber = (String) addressInformation.get("houseNumber");								
								
								String finalAddress = null;
								if (street != null && !street.equals("") && houseNumber != null && !houseNumber.equals("") ) {
									finalAddress = street + " " + houseNumber;									
								} else {
									if (street != null && !street.equals("")) {
										finalAddress = street;									
									}
									if (houseNumber != null && !houseNumber.equals("")) {
										finalAddress = houseNumber;									
									}																										
								}
								
								if (finalAddress != null) {
									org.setAddress(finalAddress);
									org.setAddressSources(sources);
								}
								
								String town = (String) addressInformation.get("town");
								if (town != null && !town.equals("")) {
									String cleanCity = town.substring(0, 1).toUpperCase() + town.substring(1);
									org.setCity(cleanCity);
								}
								
								org.setCountry("Switzerland");
								org.setCountryCode("CH");
								
								JSONArray zipCodes = (JSONArray) addressInformation.get("_value_1");
								if (zipCodes != null) {
									Iterator<JSONObject> itZip = zipCodes.iterator();
									while (itZip.hasNext()) {
										JSONObject zipData = itZip.next();
										Long swissZipCode = (Long) zipData.get("swissZipCode");										
										if (swissZipCode != null) {
											org.setPostcode(""+swissZipCode);
											break;
										}
									}
								}								
							}
						}	
					}
				}					
				// email always []				
			}
			
			JSONObject organisationIdentification = (JSONObject) organisation.get("organisationIdentification");
			if (organisationIdentification != null) {
				
				String organisationName = (String) organisationIdentification.get("organisationName");
				if (organisationName != null) {
					org.setLabel(organisationName);
				}
				
			}		
		}

		return org;

	}							

	public void importOrganizations(EntityManager entitymanager, List<Source> sources) {

		JSONParser parser = new JSONParser();
		entitymanager.getTransaction().begin();
		int numOrganizations = 0;
		
		// read JSON file
		try {

			Object fileObj = parser.parse(this.dataReader);
			JSONArray orgsArray = (JSONArray) fileObj;
			Iterator<JSONObject> it = orgsArray.iterator();
			
			// read JSON file line by line
			while (it.hasNext()) {
				
				if ((numOrganizations%100) == 0) {
					System.out.println(numOrganizations);
				}
				
				numOrganizations+=1;
								
				// JSON data row
				JSONObject orgData = it.next();

				// set main organization fields
				Organization org = setOrganizationFields(orgData, sources);								

				// connect the source to the related organization
				if (org != null) {
					org.setSources(sources);
				}				
				
				// set unique identifier
				if (org != null) {
		    	  	String identifier = sourceName + "::" + org.getLabel();
					OrganizationIdentifier orgId = new OrganizationIdentifier();
					orgId.setIdentifier(identifier);
					orgId.setProvenance(sourceName);
					orgId.setIdentifierName("lid");
					orgId.setVisibility(false);
					orgId.setOrganization(org);
					entitymanager.persist(orgId);
				}													
				
				if (org != null) {
					entitymanager.persist(org);
				}							

			}

			this.dataReader.close();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		entitymanager.getTransaction().commit();

	}	

	public void importData(String db) {

		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();

		// get or create FOEN source
		// ---------------------------------------------------------------
		entitymanager.getTransaction().begin();
		Query query = entitymanager.createQuery("Select s FROM Source s where s.label = :label");
		query.setParameter("label", sourceName);
		List<Source> foenSource = query.getResultList();
		Source source = null;
		if (foenSource.size() > 0) {
			source = foenSource.get(0);
			System.out.println("Retrieved " + source.getLabel() + " source");
		} else {
			source = setSource();
			entitymanager.persist(source);
			System.out.println("Created " + source.getLabel() + " source");
		}
		List<Source> sources = new ArrayList<Source>();
		sources.add(source);
		entitymanager.getTransaction().commit();
		// ----------------------------------------------------------------------------------------

		importOrganizations(entitymanager, sources);
		
		entitymanager.close();
		emfactory.close();
		
		System.out.println("Importer completed.");
	}

}
