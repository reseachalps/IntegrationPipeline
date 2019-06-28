package it.unimore.alps.exporter;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.github.cliftonlabs.json_simple.JsonArray;
import com.github.cliftonlabs.json_simple.JsonObject;

import it.unimore.alps.sql.model.Badge;
import it.unimore.alps.sql.model.Leader;
import it.unimore.alps.sql.model.Link;
import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationActivity;
import it.unimore.alps.sql.model.OrganizationExtraField;
import it.unimore.alps.sql.model.OrganizationIdentifier;
import it.unimore.alps.sql.model.OrganizationRelation;
import it.unimore.alps.sql.model.Person;
import it.unimore.alps.sql.model.PersonIdentifier;
import it.unimore.alps.sql.model.Project;
import it.unimore.alps.sql.model.ProjectIdentifier;
import it.unimore.alps.sql.model.Publication;
import it.unimore.alps.sql.model.PublicationIdentifier;
import it.unimore.alps.sql.model.Source;
import it.unimore.alps.sql.model.Thematic;
import it.unimore.alps.utils.CSVFileWriter;

public class NewExporterCSVOrganization {

	public static void main(String[] args) {	
		
		CommandLine commandLine;			
        Option groubByDimensionOption = Option.builder("groubByDimension")
        		.hasArg()
        		.required(true)
        		.desc("The organization field throught which the data will be grouped.")
        		.longOpt("groubByDimension")
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

        options.addOption(groubByDimensionOption);
        options.addOption(DB);
        options.addOption(outputFolderOption);
        
        
        String groubByDimension = null;
        String db = null;
        String outputFolder = null;
                	
        try {
			commandLine = parser.parse(options, args);
		
			System.out.println("----------------------------");
			System.out.println("OPTIONS:");
			
	        if (commandLine.hasOption("groubByDimension")) {   
	        	groubByDimension = commandLine.getOptionValue("groubByDimension");
	        	
	        	// check that the groupByDimension is included among the Organization field
	        	if (groubByDimension.equals("All")) {
	        		System.out.println("\tGroup by dimension: " + groubByDimension);
	        	} else {
		    		Class clazz;
		    		boolean found = false;
		    		try {
		    			clazz = Class.forName("it.unimore.alps.sql.model.Organization");
		    			for (Field field : clazz.getDeclaredFields()) {
			                if (field.getName().equals(groubByDimension)) {
			                	found = true;
			                	break;
			                }
		    			}
		    			if (!found) {
		    				System.out.println("\tGroup by dimension not included in the Organization table.");
		    				System.exit(1);
		    			}
		    			System.out.println("\tGroup by dimension: " + groubByDimension);
		    			/*Field field = clazz.getField(groubByDimension);
		    			if (field == null) {
		    				System.out.println("\tThe attribute " + groubByDimension + " is not included in the Organization class.");
		    	        	System.exit(1); 
		    			}*/
		    		//} catch (ClassNotFoundException | NoSuchFieldException | SecurityException e) {
		    		} catch (ClassNotFoundException | SecurityException e) {
		    			// TODO Auto-generated catch block
		    			e.printStackTrace();
		    			System.exit(1);
		    		}
	        	}	
	        	
			} else {
				System.out.println("\tGroub by dimension not provided. Use the groubByDimension option.");
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
        
        final String outdir = outputFolder;
        final String dbName = db;
		
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();
		
		// retrieve all the organizations
		entitymanager.getTransaction().begin();			
		List<Organization> allOrgs = retrieveAllOrganizations(entitymanager);
		entitymanager.getTransaction().commit();
		
		if (groubByDimension.equals("All")) {
			// export organizations
        	System.out.println("Exporting all the organizations...");
        	List<List<String>> exportedOrganization = exportOrganizations(allOrgs);                    	
        	CSVFileWriter writer = new CSVFileWriter(outdir+"organizations_" + db + ".csv");
            writer.write(exportedOrganization);
            System.out.println("Organizations exporting completed.");
		} else {
				
			// perform a group by of the organizations on the basis of the input "groupByDimension" parameter
			HashMap<String, List<Organization>> groupedOrgs = new HashMap<>();
			for (Organization org: allOrgs) {
				try {
					
					String value = (String) getEntityAttributeValue(org, groubByDimension);
					
					if (!groupedOrgs.containsKey(value)) {
						List<Organization> initOrgs = new ArrayList<>();
						initOrgs.add(org);
						groupedOrgs.put(value, initOrgs);
					} else {
						List<Organization> oldOrgs = groupedOrgs.get(value);
						oldOrgs.add(org);
						groupedOrgs.put(value, oldOrgs);
					}
					
					
				} catch (NoSuchFieldException | IllegalArgumentException | IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			
			for (String groupedValue : groupedOrgs.keySet()) {
				List<Organization> groupedOrgsSingleValue = groupedOrgs.get(groupedValue);
				// export organizations
	        	System.out.println("[" + groubByDimension + "=" + groupedValue +"] Exporting organizations...");
	        	List<List<String>> exportedOrganization = exportOrganizations(groupedOrgsSingleValue);                    	
	        	CSVFileWriter writer = new CSVFileWriter(outdir+"organizations_" + groubByDimension + "_" + groupedValue + ".csv");
	            writer.write(exportedOrganization);
	            System.out.println("[" + groubByDimension + "=" + groupedValue +"] Organizations exporting completed.");
				
			}
		}	
        
		entitymanager.close();
		emfactory.close();
		
	}	
	
	
	private static Object getEntityAttributeValue(Object entity, String attr) throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		
		Class<?> entityClass = entity.getClass();
		Field f = entityClass.getDeclaredField(attr);
		f.setAccessible(true);
		Object value = (Object) f.get(entity);								
		
		return value;
	}
	
	
	public static List<Organization> retrieveAllOrganizations(EntityManager entitymanager) {
		
    	System.out.println("Starting retrieving all the organizations");
        Query query_org = entitymanager.createQuery("Select o FROM Organization o");
        List<Organization> orgs = query_org.getResultList();        
        System.out.println("Retrieved " + orgs.size() + " organizations");
        
        return orgs;
	}			
	
	
	public static List<List<String>> exportOrganizations(List<Organization> organizations) {

		List<List<String>> orgsData = new ArrayList<List<String>>();
		
		/*List<String> header = new ArrayList<>(Arrays.asList("id", "acronyms", "alias", "label", "creationYear",
				"commercialLabel", "address", "city", "citycode", "country", "countryCode", "postcode", "urbanUnit",
				"urbanUnitCode", "lat", "lon", "sources", "addressSources", "revenueRange", "privateFinanceDate", "employees", "typeCategoryCode",
				"typeLabel", "typeKind", "isPublic", "leaders", "staff", "links", "privateOrgTypeId", "privateOrgTypeLabel",
				"activities", "relations", "badges", "children", "identifiers"));*/
		List<String> header = new ArrayList<>(Arrays.asList("id", "acronyms", "alias", "label", "creationYear",
				"commercialLabel", "address", "city", "citycode", "country", "countryCode", "postcode", "urbanUnit",
				"urbanUnitCode", "lat", "lon", "revenueRange", "privateFinanceDate", "employees", "typeCategoryCode",
				"typeLabel", "typeKind", "isPublic", "leaders", "staff", "links", "privateOrgTypeId", "privateOrgTypeLabel",
				"activities", "relations", "badges", "children", "identifiers"));		
		
		orgsData.add(header);

		int numOrgs = 0;
		for (Organization org : organizations) {
			/*if (numOrgs%1000 == 0) {
				System.out.println(numOrgs);
			}*/
			
			List<String> orgData = new ArrayList<String>();
			
			orgData.add(Integer.toString(org.getId()));
			//orgData.add(org.getAcronym());
			saveAcronyms(org, orgData);
			
			orgData.add(org.getAlias());
			orgData.add(org.getLabel());
			
			if (org.getCreationYear() != null) {
				orgData.add(org.getCreationYear().toString());
			} else {
				orgData.add("");
			}

			orgData.add(org.getCommercialLabel());

			// address
			saveAddress(org, orgData);
			
			/*// sources
			List<String> sources = new ArrayList<String>();
			for (Source source: org.getSources()) {
				if (source.getRevisionDate() != null) {
					sources.add(source.getLabel() + "_" + source.getUrl() + "_" + source.getRevisionDate().toString());
				} else {
					sources.add(source.getLabel() + "_" + source.getUrl() + "_null");
				}
			}
			orgData.add(String.join(";", sources));
			
			// addressSource
			List<String> addressSources = new ArrayList<String>();
			for (Source aSource: org.getAddressSources()) {
				if (aSource.getRevisionDate() != null) {
					addressSources.add(aSource.getLabel() + "_" + aSource.getUrl() + "_" + aSource.getRevisionDate().toString());
				} else {
					addressSources.add(aSource.getLabel() + "_" + aSource.getUrl() + "_null");
				}
			}
			orgData.add(String.join(";", addressSources));	*/			

			// private finance
			orgData.add(org.getFinancePrivateRevenueRange());
			if (org.getFinancePrivateDate() != null) {
				orgData.add(org.getFinancePrivateDate().toString());
			} else {
				orgData.add("");
			}
			orgData.add(org.getFinancePrivateEmployees());
								
			orgData.add(org.getTypeCategoryCode());
			orgData.add(org.getTypeLabel());
			orgData.add(org.getTypeKind());
			//orgData.add(Integer.toString(org.getIsPublic()));			
			orgData.add(org.getIsPublic());

			// leaders
			saveLeaders(org, orgData);
			
			// staff
			savePeople(org, orgData);

			// links
			saveLinks(org, orgData);

			// companytype
			savePrivateOrganizationType(org, orgData);

			// activities
			saveActivities(org, orgData);

			// relations
			saveRelations(org, orgData);

			// badges
			saveBadges(org, orgData);
			
			// children organizations
			saveOrganizationChildren(org, orgData);	
			
			// organization identifiers
			List<String> orgIds = new ArrayList<String>();
			for (OrganizationIdentifier orgId : org.getOrganizationIdentifiers()) {
				String identifier = orgId.getIdentifierName().replace(" ", ""); 
				orgIds.add(identifier + "_" + orgId.getIdentifier());						
			}			
			orgData.add(String.join(";", orgIds));	
			
			// label organization if it contains ERC code
			/*if (orgContainsERCCode(org)) {
				orgData.add("1");
			} else {
				orgData.add("0");
			}*/

			orgsData.add(orgData);
			numOrgs++;

		}
		
		// TODO: export link sources, leader sources and person sources ?
		// TODO: export visibility ?
		// TODO: export extra fields in a single column ?
		
		return orgsData;

	}
	
	private static void saveAddress(Organization org, List<String> orgData) {
		
		orgData.add(org.getAddress());
		orgData.add(org.getCity());
		orgData.add(org.getCityCode());
		orgData.add(org.getCountry());
		orgData.add(org.getCountryCode());
		orgData.add(org.getPostcode());
		orgData.add(org.getUrbanUnit());
		orgData.add(org.getUrbanUnitCode());
		orgData.add(Float.toString(0));
		orgData.add(Float.toString(0));
		/*if (org.getLat() > 0) {
			orgData.add(Float.toString(org.getLat()));
		} else {
			orgData.add(Float.toString(0));
		}
		if (org.getLon() > 0) {
			orgData.add(Float.toString(org.getLon()));
		} else {
			orgData.add(Float.toString(0));
		}*/

	}
	
	private static void saveAcronyms(Organization org, List<String> orgData) {
		
		List<String> acronyms = new ArrayList<String>();

		for (String acronym: org.getAcronyms()) {
			acronyms.add(acronym);						
		}
		
		orgData.add(String.join(";", acronyms));		

	}
	
	private static void saveLinks(Organization org, List<String> orgData) {
		
		List<String> links = new ArrayList<String>();
		for (Link link : org.getLinks()) {
			links.add(link.getUrl());
		}
		orgData.add(String.join(";", links));

	}
	
	private static void savePrivateOrganizationType(Organization org, List<String> orgData) {
		
		if (org.getOrganizationType() != null) {
			orgData.add(Integer.toString(org.getOrganizationType().getId()));
			orgData.add(org.getOrganizationType().getLabel());
		} else {
			orgData.add("");
			orgData.add("");
		}	
		
	}
	
	private static void saveActivities(Organization org, List<String> orgData) {
		
		List<String> activities = new ArrayList<String>();
		
		for (OrganizationActivity act : org.getActivities()) {
			activities.add(act.getType() + "_" + act.getCode() + "_" + act.getLabel());
		}
		
		orgData.add(String.join(";", activities));
		
	}
	
	private static boolean orgContainsERCCode(Organization org) {
		
		boolean found = false;
		for (OrganizationActivity act : org.getActivities()) {
			if (act.getType().equals("ERC")) {
				found = true;
				break;
			}
		}
		
		return found; 
		
	}
	
	private static void saveRelations(Organization org, List<String> orgData) {
		
		List<String> ids = new ArrayList<String>();

		for (OrganizationRelation rel : org.getOrganizationRelations()) {
			ids.add(Integer.toString(rel.getId()));						
		}
		
		orgData.add(String.join(";", ids));

	}
	
	private static void saveBadges(Organization org, List<String> orgData) {
		
		List<String> badges = new ArrayList<String>();

		for (Badge badge : org.getBadges()) {
			badges.add(badge.getLabel());						
		}
		
		orgData.add(String.join(";", badges));
		
	}
	
	private static void saveLeaders(Organization org, List<String> orgData) {
		
		List<String> leaders = new ArrayList<String>();

		for (Leader person : org.getLeaders()) {
			leaders.add(person.getFirstName() + " " + person.getLastName());						
		}
		
		orgData.add(String.join(";", leaders));		

	}
	
	private static void savePeople(Organization org, List<String> orgData) {
		
		List<String> leaders = new ArrayList<String>();

		for (Person person : org.getPeople()) {
			leaders.add(person.getFirstName() + " " + person.getLastName());						
		}
		
		orgData.add(String.join(";", leaders));		

	}
	
	private static void saveOrganizationChildren(Organization org, List<String> orgData) {
		
		List<String> children = new ArrayList<String>();

		for (Organization orgC : org.getChildrenOrganizations()) {
			children.add(orgC.getId() + "_" + orgC.getLabel());						
		}
		
		orgData.add(String.join(";", children));	
		
	}			

}
