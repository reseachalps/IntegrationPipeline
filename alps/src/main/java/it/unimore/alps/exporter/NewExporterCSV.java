package it.unimore.alps.exporter;

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

public class NewExporterCSV {

	public static void main(String[] args) {
		
		List<String> sourceNames = new ArrayList<String>(Arrays.asList("All", "Arianna - Anagrafe Nazionale delle Ricerche", "Questio", "OpenAire", "CercaUniversita", "Bureau van Dijk", "Consiglio Nazionale delle Ricerche (CNR)", "ScanR", "ORCID", "Patiris", "Startup - Registro delle imprese", "Grid", "P3"));
		
		CommandLine commandLine;	
		
        Option sourceNameOption = Option.builder("sourceName")
        		.hasArg()
        		.required(true)
        		.desc("The source to be exported. The possible choice are: All, Arianna - Anagrafe Nazionale delle Ricerche, Questio, OpenAire, CercaUniversita, Bureau van Dijk, Consiglio Nazionale delle Ricerche (CNR), ScanR, ORCID, Patiris, Startup - Registro delle imprese, Grid and P3.")
        		.longOpt("sourceName")
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

        options.addOption(sourceNameOption);
        options.addOption(DB);
        options.addOption(outputFolderOption);
        
        
        String sourceName = null;
        String db = null;
        String outputFolder = null;
                	
        try {
			commandLine = parser.parse(options, args);
		
			System.out.println("----------------------------");
			System.out.println("OPTIONS:");
			
	        if (commandLine.hasOption("sourceName")) {   
	        	sourceName = commandLine.getOptionValue("sourceName");
	        	
	        	if (!sourceNames.contains(sourceName)) {
	        		System.out.println("\tWrong source name. The possible choice are: All, Arianna - Anagrafe Nazionale delle Ricerche, Questio, OpenAire, CercaUniversita, Bureau van Dijk, Consiglio Nazionale delle Ricerche (CNR), ScanR, ORCID, Patiris, Startup - Registro delle imprese, Grid and P3.");
	        	} else {
	        		if (sourceName.equals("All")) {
	        			System.out.println("\tExport all sources.");
	        		} else {
	        			System.out.println("\tSource to be exported: " + sourceName);
	        		}
	        	}	        	
	        	
			} else {
				System.out.println("\tSource name not provided. Use the sourceName option.");
	        	System.exit(1);
			}	
	        
	        
	        
	        
	        if(commandLine.hasOption("outputFolder")) {
	        	
	        	outputFolder =commandLine.getOptionValue("outputFolder");
    	
	        	
	        }else {
	        	System.out.println("\tOutput Folder is not provided. Use the DB option.");
	        	System.exit(1);
	        }
	        
	        
	        
	        
	        if(commandLine.hasOption("DB")) {
	        	
	        	db =commandLine.getOptionValue("DB");
    	
	        	
	        }else {
	        	System.out.println("\tDB name not provided. Use the outputFolder option.");
	        	System.exit(1);
	        }
	        
			
			System.out.println("----------------------------\n");
        
        } catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
        
        // -sourceName <sourcename_or_all>
        
        final String outdir = outputFolder;
        final String dbName = db;
		
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();
		
		entitymanager.getTransaction().begin();
		Query query;
		if(sourceName.equals("All")) {
			query = entitymanager.createQuery("Select s FROM Source s");
		} else {
			query = entitymanager.createQuery("Select s FROM Source s where s.label=:sourceLabel");
			query.setParameter("sourceLabel", sourceName);
		}

        List<Source> sources = query.getResultList();
        entitymanager.getTransaction().commit();
        
        sources.parallelStream().forEach(new Consumer<Source>() {
			
			@Override
			public void accept(Source source) {
				
				EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(dbName);
				EntityManager entitymanager = emfactory.createEntityManager();
				
				entitymanager.getTransaction().begin();
				
				System.out.println("[" + source.getLabel() +"] Starting the exporter...");
				
				if (source.getLabel().equals("ORCID")) { // retrive ORCID parent organizations
					System.out.println("[ORCID parent] Exporting organizations...");
	        		List<Organization> parentOrgs = retrieveParentOrganizations(entitymanager, source);
	            	List<List<String>> exportedOrganization = exportOrganizations(parentOrgs);
	            	CSVFileWriter writer = new CSVFileWriter(outdir+"organizations_" + source.getLabel() + "_parent.csv");
	                writer.write(exportedOrganization);
	                System.out.println("[ORCID parent] Exporter completed.");
	        	}
	        	
	        	// retrieve entities        	        	
	        	List<Organization> orgs = retrieveOrganizations(entitymanager, source);
	        	List<Project> prjs = retrieveProjects(entitymanager, source);
	        	List<Publication> pubs = retrievePublications(entitymanager, source);
	        	//List<Person> people = retrievePeople(entitymanager, source);
	     	      	
	        	// export organizations
	        	System.out.println("[" + source.getLabel() +"] Exporting organizations...");
	        	List<List<String>> exportedOrganization = exportOrganizations(orgs);                    	
	            //CSVFileWriter writer = new CSVFileWriter("/home/matteo/Scrivania/organizations_" + source.getLabel() + ".csv");
	        	CSVFileWriter writer = new CSVFileWriter(outdir+"organizations_" + source.getLabel() + ".csv");
	            writer.write(exportedOrganization);
	            System.out.println("[" + source.getLabel() +"] Organizations exporting completed.");
	            
	            // export projects 
	            System.out.println("[" + source.getLabel() +"] Exporting projects...");
	            List<List<String>> exportedProjects = exportProjects(prjs, entitymanager, source, orgs, pubs);             
	            //writer = new CSVFileWriter("/home/matteo/Scrivania/projects_" + source.getLabel() + ".csv");
	            writer = new CSVFileWriter(outdir+"projects_" + source.getLabel() + ".csv");
	            writer.write(exportedProjects);
	            System.out.println("[" + source.getLabel() +"] Projects exporting completed.");
	            
	            System.out.println("[" + source.getLabel() +"] Exporter completed.");
				
				/*// CUSTOM EXPORTING FOR DISSEMINATION: organizations and people contacts in separate files
				// retrieve entities        	        	
	        	List<Organization> orgs = retrieveOrganizations(entitymanager, source);
	        	List<Project> prjs = retrieveProjects(entitymanager, source);
	        	List<Publication> pubs = retrievePublications(entitymanager, source);
	        	List<Leader> leaders = retrieveLeaders(entitymanager, source);
	     	      	
	        	// export organizations
	        	System.out.println("[" + source.getLabel() +"] Exporting organizations...");
	        	List<List<String>> exportedOrganization = exportCustomOrganizations(orgs);                    	
	            //CSVFileWriter writer = new CSVFileWriter("/home/matteo/Scrivania/organizations_" + source.getLabel() + ".csv");
	        	CSVFileWriter writer = new CSVFileWriter(outdir+"organizations_" + source.getLabel() + ".csv");
	            writer.write(exportedOrganization);
	            System.out.println("[" + source.getLabel() +"] Organizations exporting completed.");
	            
	            // export people            
	            List<List<String>> exportedPeople = exportLeaders(leaders, orgs);             
	            //writer = new CSVFileWriter("/home/matteo/Scrivania/people_" + source.getLabel() + ".csv");
	            writer = new CSVFileWriter(outdir+"people_" + source.getLabel() + ".csv");
	            writer.write(exportedPeople);
	            System.out.println("[" + source.getLabel() +"] People exporting completed.");*/
	            
	            
	            // OTHER CUSTOM EXPORTING FOR DISSEMINATION: organization and people contacts in same file
	            // retrieve entities
	            /*List<Organization> orgs = retrieveOrganizations(entitymanager, source);
	            // export organizations
	        	System.out.println("[" + source.getLabel() +"] Exporting organizations...");
	        	List<List<String>> exportedOrganization = exportOrganizationsWithPeople(orgs);                    	
	            //CSVFileWriter writer = new CSVFileWriter("/home/matteo/Scrivania/organizations_" + source.getLabel() + ".csv");
	        	CSVFileWriter writer = new CSVFileWriter(outdir+"organizations_and_people_" + source.getLabel() + ".csv");
	            writer.write(exportedOrganization);
	            System.out.println("[" + source.getLabel() +"] Organizations exporting completed.");*/
	            
	            
	            entitymanager.getTransaction().commit();
			}
        });
               
        /*for(Source source: sources) {
        	
        	if (source.getLabel().equals("ORCID")) { // retrive ORCID parent organizations
        		List<Organization> parentOrgs = retrieveParentOrganizations(entitymanager, source);
            	List<List<String>> exportedOrganization = exportOrganizations(parentOrgs);
            	CSVFileWriter writer = new CSVFileWriter(outputFolder+"organizations_" + source.getLabel() + "_parent.csv");
                writer.write(exportedOrganization);
        	}
        	
        	// retrieve entities        	        	
        	List<Organization> orgs = retrieveOrganizations(entitymanager, source);
        	List<Project> prjs = retrieveProjects(entitymanager, source);
        	List<Publication> pubs = retrievePublications(entitymanager, source);
        	//List<Person> people = retrievePeople(entitymanager, source);
     	      	
        	// export organizations        	       	            
        	List<List<String>> exportedOrganization = exportOrganizations(orgs);                    	
            //CSVFileWriter writer = new CSVFileWriter("/home/matteo/Scrivania/organizations_" + source.getLabel() + ".csv");
        	CSVFileWriter writer = new CSVFileWriter(outputFolder+"organizations_" + source.getLabel() + ".csv");
            writer.write(exportedOrganization);
            
            // export projects            
            List<List<String>> exportedProjects = exportProjects(prjs, entitymanager, source, orgs, pubs);             
            //writer = new CSVFileWriter("/home/matteo/Scrivania/projects_" + source.getLabel() + ".csv");
            writer = new CSVFileWriter(outputFolder+"projects_" + source.getLabel() + ".csv");
            writer.write(exportedProjects);
            
            //// export publications            
            /*List<List<String>> exportedPublications = exportPublications(pubs, entitymanager);             
            //writer = new CSVFileWriter("/home/matteo/Scrivania/publications_" + source.getLabel() + ".csv");
            writer = new CSVFileWriter("/home/matteop/publications_" + source.getLabel() + ".csv");
            writer.write(exportedPublications);
            
            // export people            
            List<List<String>> exportedPeople = exportPeople(people, entitymanager, source, orgs);             
            //writer = new CSVFileWriter("/home/matteo/Scrivania/people_" + source.getLabel() + ".csv");
            writer = new CSVFileWriter("/home/matteop/people_" + source.getLabel() + ".csv");
            writer.write(exportedPeople);
        	
        }*/
        
		entitymanager.close();
		emfactory.close();
		
	}
	
	public static List<Organization> retrieveParentOrganizations(EntityManager entitymanager, Source source) {
		
		//entitymanager.getTransaction().begin();
    	System.out.println("[" + source.getLabel() +"] Starting retrieving parent organizations deriving from " + source.getLabel() + " source");
        Query query_org = entitymanager.createQuery("Select o FROM Organization o WHERE :id MEMBER OF o.sources");
        query_org.setParameter("id", source);
        List<Organization> orgs = query_org.getResultList();
        
        List<Organization> parentOrgs = new ArrayList<>();
        for(Organization org: orgs) {
        	if(org.getChildrenOrganizations() != null) {
        		if (org.getChildrenOrganizations().size() > 0) {
        			parentOrgs.add(org);
        		}
        	}
        }
        
        //entitymanager.getTransaction().commit();
        System.out.println("[" + source.getLabel() + "] Retrieved " + parentOrgs.size() + " parent organizations deriving from " + source.getLabel() + " source");
        
        return parentOrgs;
	}
	
	public static List<Organization> retrieveOrganizations(EntityManager entitymanager, Source source) {
		
		//entitymanager.getTransaction().begin();
    	System.out.println("[" + source.getLabel() + "] Starting retrieving organizations deriving from " + source.getLabel() + " source");
        Query query_org = entitymanager.createQuery("Select o FROM Organization o WHERE :id MEMBER OF o.sources");
        query_org.setParameter("id", source);
        List<Organization> orgs = query_org.getResultList();
        
        //entitymanager.getTransaction().commit();
        System.out.println("[" + source.getLabel() + "] Retrieved " + orgs.size() + " organizations deriving from " + source.getLabel() + " source");
        
        return orgs;
	}
	
	public static List<Project> retrieveProjects(EntityManager entitymanager, Source source) {
		
		//entitymanager.getTransaction().begin();
        System.out.println("[" + source.getLabel() + "] Starting retrieving projects deriving from " + source.getLabel() + " source");
        Query query_prj = entitymanager.createQuery("Select o FROM Project o WHERE :id MEMBER OF o.sources");
        query_prj.setParameter("id", source);
        //Query query_prj = entitymanager.createQuery("Select o FROM Project o");        
        List<Project> prjs = query_prj.getResultList();
        
        //entitymanager.getTransaction().commit();
        System.out.println("[" + source.getLabel() + "] Retrieved " + prjs.size() + " projects deriving from " + source.getLabel() + " source");
        
        return prjs;
	}
	
	public static List<Publication> retrievePublications(EntityManager entitymanager, Source source) {
		
		//entitymanager.getTransaction().begin();
        System.out.println("[" + source.getLabel() + "] Starting retrieving publications deriving from " + source.getLabel() + " source");
        Query query_prj = entitymanager.createQuery("Select o FROM Publication o WHERE :id MEMBER OF o.sources");
        query_prj.setParameter("id", source);
        //Query query_prj = entitymanager.createQuery("Select o FROM Project o");        
        List<Publication> pubs = query_prj.getResultList();
        
        //entitymanager.getTransaction().commit();
        System.out.println("[" + source.getLabel() + "] Retrieved " + pubs.size() + " publications deriving from " + source.getLabel() + " source");
        
        return pubs;
	}
	
	public static List<Person> retrievePeople(EntityManager entitymanager, Source source) {
		
		//entitymanager.getTransaction().begin();
        System.out.println("[" + source.getLabel() + "] Starting retrieving people deriving from " + source.getLabel() + " source");
        Query query_prj = entitymanager.createQuery("Select o FROM Person o WHERE :id MEMBER OF o.sources");
        query_prj.setParameter("id", source);
        //Query query_prj = entitymanager.createQuery("Select o FROM Project o");        
        List<Person> people = query_prj.getResultList();
        
        //entitymanager.getTransaction().commit();
        System.out.println("[" + source.getLabel() + "] Retrieved " + people.size() + " people deriving from " + source.getLabel() + " source");
        
        return people;
	}
	
	public static List<Leader> retrieveLeaders(EntityManager entitymanager, Source source) {
		
		//entitymanager.getTransaction().begin();
        System.out.println("[" + source.getLabel() + "] Starting retrieving leaders deriving from " + source.getLabel() + " source");
        Query query_prj = entitymanager.createQuery("Select o FROM Leader o WHERE :id MEMBER OF o.sources");
        query_prj.setParameter("id", source);
        //Query query_prj = entitymanager.createQuery("Select o FROM Project o");        
        List<Leader> leaders = query_prj.getResultList();
        
        //entitymanager.getTransaction().commit();
        System.out.println("[" + source.getLabel() + "] Retrieved " + leaders.size() + " leaders deriving from " + source.getLabel() + " source");
        
        return leaders;
	}
	
	private static List<List<String>> exportOrganizationsWithPeople(List<Organization> organizations) {
		
		List<List<String>> orgsData = new ArrayList<List<String>>();
		
		List<String> header = new ArrayList<>(Arrays.asList("org_id", "org_name", "org_websites", "org_city", "org_country", "org_type", "person_firstname", "person_lastname", "person_title", "person_email"));
		
		orgsData.add(header);

		for (Organization org : organizations) {
			
			List<Leader> leaders = org.getLeaders();
			if (leaders != null && leaders.size() > 0) {
				for(Leader l: leaders) {
			
					List<String> orgData = new ArrayList<String>();
					
					orgData.add(""+org.getId());
					
					orgData.add(org.getLabel());
					
					// links
					saveLinks(org, orgData);
					
					String city = org.getCity();
					if (city != null) {
						orgData.add(city);
					} else {
						orgData.add("");
					}
					
					String country = org.getCountry();
					if (country != null) {
						orgData.add(country);
					} else {
						orgData.add("");
					}
					
					orgData.add(org.getIsPublic());
									
					orgData.add(l.getFirstName());
					orgData.add(l.getLastName());
					orgData.add(l.getTitle());
					orgData.add(l.getEmail());
	
					orgsData.add(orgData);
				}
				
			}						

		}		
		
		return orgsData;
		
	}
	
	private static List<List<String>> exportCustomOrganizations(List<Organization> organizations) {
		
		List<List<String>> orgsData = new ArrayList<List<String>>();
		
		List<String> header = new ArrayList<>(Arrays.asList("id", "label", "address", "postcode", "city", "citycode", "urbanUnit", "urbanUnitCode", "country", "countryCode", "Telefono", "Email", "links", "Proprietà (Pubblico, Privato)", "Tipologia Specifica Ente"));
		
		orgsData.add(header);

		for (Organization org : organizations) {
			
			List<String> orgData = new ArrayList<String>();
			
			orgData.add(""+org.getId());
			
			orgData.add(org.getLabel());

			orgData.add(org.getAddress());
			orgData.add(org.getPostcode());			
			orgData.add(org.getCity());
			orgData.add(org.getCityCode());
			orgData.add(org.getUrbanUnit());
			orgData.add(org.getUrbanUnitCode());
			orgData.add(org.getCountry());
			orgData.add(org.getCountryCode());
			
			Map<String, List<String>> extraMap = new HashMap<>(); 
			for (OrganizationExtraField oef: org.getOrganizationExtraFields()) {
				if (extraMap.get(oef.getFieldKey()) != null) {
					List<String> commonExtraFields = extraMap.get(oef.getFieldKey());
					commonExtraFields.add(oef.getFieldValue().trim());
					extraMap.put(oef.getFieldKey(), commonExtraFields);
				} else {
					List<String> commonExtraFields = new ArrayList<>();
					commonExtraFields.add(oef.getFieldValue().trim());
					extraMap.put(oef.getFieldKey(), commonExtraFields);
				}
			}
			
			// telefono
			List<String> phones = new ArrayList<>();
			// Arianna
			if (extraMap.containsKey("TELEFONO")) {
				String phone = null;
				if (extraMap.containsKey("PREFISSO")) {
					phone = extraMap.get("PREFISSO").get(0) + extraMap.get("TELEFONO").get(0);
				} else {
					phone = extraMap.get("TELEFONO").get(0);
				}
				phones.add(phone);
			}
			// bvd and patiris
			if (extraMap.containsKey("phone")) {				
				phones.addAll(extraMap.get("phone"));
			}
			// cercauniversità and questio
			if (extraMap.containsKey("Telefono")) {
				phones.addAll(extraMap.get("Telefono"));
			}
			// cnr
			if (extraMap.containsKey("telefono")) {
				phones.add(extraMap.get("telefono").get(0));
			}
			if (phones.size() > 0) {
				orgData.add(String.join(";", phones));
			} else {
				orgData.add("");
			}
			
			
			List<String> emails = new ArrayList<>();
			// Arianna			
			if (extraMap.containsKey("EMAIL")) {
				emails.add(extraMap.get("EMAIL").get(0));
			}
			// bvd and cnr (part 1)
			if (extraMap.containsKey("email")) {
				emails.addAll(extraMap.get("email"));
			}
			// cnr (part 2)
			if (extraMap.containsKey("E-mail")) {
				emails.add(extraMap.get("E-mail").get(0));
			}
			if (emails.size() > 0) {
				orgData.add(String.join(";", emails));
			} else {
				orgData.add("");
			}
								
			// links
			saveLinks(org, orgData);
			
			String isPublic = org.getIsPublic();
			if (isPublic.equals("true")) {
				orgData.add("Pubblico");
			} else {
				if (isPublic.equals("false")) {
					orgData.add("Privato");
				} else {
					orgData.add(isPublic);
				}
			}						
			
			// companytype
			if (org.getOrganizationType() != null) {
				orgData.add(org.getOrganizationType().getLabel());
			} else {
				orgData.add("");
			}

			orgsData.add(orgData);

		}		
		
		return orgsData;
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
				"urbanUnitCode", "lat", "lon", "nuts1", "nuts2", "nuts3", "revenueRange", "privateFinanceDate", "employees", "typeCategoryCode",
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
			
			orgData.add(org.getNutsLevel1());
			orgData.add(org.getNutsLevel2());
			orgData.add(org.getNutsLevel3());
			
			
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
	
	
	/*private static List<Organization> retrieveOrganizationsBySource(EntityManager em, Source source) {
		em.getTransaction().begin();
		Query query = em.createQuery("SELECT o FROM Organization o WHERE :id MEMBER OF o.sources");
		query.setParameter("id", source);
		em.getTransaction().commit();
		List<Organization> sourceOrgs = query.getResultList();
		return sourceOrgs;
	}
	
	private static List<Publication> retrievePublicationsBySource(EntityManager em, Source source) {
		em.getTransaction().begin();
		Query query = em.createQuery("SELECT p FROM Publication p WHERE :id MEMBER OF p.sources");
		query.setParameter("id", source);
		em.getTransaction().commit();
		List<Publication> sourcePubs = query.getResultList();
		return sourcePubs;
	}*/
	
	public static List<List<String>> exportProjects(List<Project> projects, EntityManager em, Source source, List<Organization> sourceOrgs, List<Publication> sourcePubs) {
		
		List<List<String>> prjsData = new ArrayList<List<String>>();
		
		List<String> header = new ArrayList<>(Arrays.asList("id", "acronym", "label", "description", "duration",
				"budget", "call", "callLabel", "url", "startDate", "type", "year", "month", "themes", "orgs", "pubs", "identifiers"));
		
		prjsData.add(header);
		
		Map<Integer, List<Organization>> orgPrjMap = new HashMap<>();
		Map<Integer, List<Publication>> pubPrjMap = new HashMap<>();
		if (projects.size() > 0) { 
			// create org-projects map			
			for (Organization org: sourceOrgs) {
				List<Project> orgsProjects = org.getProjects();
				if (orgsProjects != null) {
					for (Project prj: orgsProjects) {
						int key = prj.getId();
						if (orgPrjMap.get(key) != null) {
							orgPrjMap.get(key).add(org);
						} else {
							List<Organization> orgs = new ArrayList<>();
							orgs.add(org);
							orgPrjMap.put(key, orgs);
						}
					}
				}
			}
			
			// create publication-projects map			
			for (Publication pub: sourcePubs) {
				List<Project> pubsProjects = pub.getProjects();
				if (pubsProjects != null) {
					for (Project prj: pubsProjects) {
						int key = prj.getId();
						if (pubPrjMap.get(key) != null) {
							pubPrjMap.get(key).add(pub);
						} else {
							List<Publication> pubs = new ArrayList<>();
							pubs.add(pub);
							pubPrjMap.put(key, pubs);
						}
					}
				}
			}
		}
		
		int numPrj = 0;

		for (Project prj : projects) {
			
			/*if (numPrj%1000 == 0) {
				System.out.println(numPrj);
			}*/
			
			List<String> prjData = new ArrayList<String>();
			prjData.add(Integer.toString(prj.getId()));
			prjData.add(prj.getAcronym());
			String label = prj.getLabel();
			if (label != null) {
				label = label.replace("\n", " ");
				label = label.replace("\t", " ");
			} else {
				label = "";
			}
			prjData.add(label);
			prjData.add(prj.getDescription());
			prjData.add(prj.getDuration());
			prjData.add(prj.getBudget());
			prjData.add(prj.getCallId());
			prjData.add(prj.getCallLabel());
			prjData.add(prj.getUrl());
			
			if(prj.getStartDate() != null) {
				prjData.add(prj.getStartDate().toString());
			} else {
				prjData.add("");
			}
			
			prjData.add(prj.getType());
			prjData.add(prj.getYear());
			prjData.add(prj.getMonth());
			
			// themes
			saveThemes(prj, prjData);
			
			// organizations
			//List<Organization> organizations = getOrgsFromProject(sourceOrgs, prj);			
			List<Organization> organizations = orgPrjMap.get(prj.getId());
			if (organizations != null) {
				List<String> orgs = new ArrayList<String>();
				for (Organization org : organizations) {
					orgs.add(org.getId() + "_" + org.getLabel());					
				}			
				prjData.add(String.join(";", orgs));
			} else {
				prjData.add("");
			}
			
			// publications
			//List<Publication> publications = getPubsFromProject(sourcePubs, prj);
			List<Publication> publications = pubPrjMap.get(prj.getId());
			if (publications != null) {
				List<String> pubs = new ArrayList<String>();
				for (Publication pub: publications) {
					pubs.add(pub.getId() + "_" + pub.getTitle());					
				}			
				prjData.add(String.join(";", pubs));
			} else {
				prjData.add("");
			}
			
			// project identifiers
			List<String> prjIds = new ArrayList<String>();
			for (ProjectIdentifier prjId : prj.getProjectIdentifiers()) {
				String identifier = prjId.getIdentifierName().replace(" ", ""); 
				prjIds.add(identifier + "_" + prjId.getIdentifier());						
			}			
			prjData.add(String.join(";", prjIds));

			prjsData.add(prjData);
			
			numPrj++;

		}
		
		return prjsData;
	}
	
	
	public static List<List<String>> exportPublications(List<Publication> publications, EntityManager em) {

		List<List<String>> pubsData = new ArrayList<List<String>>();
		
		List<String> header = new ArrayList<>(Arrays.asList("id", "title", "subtitle", "location_type", "location_name",
				"publicationDate", "description", "url", "type", "authors", "thematics", "identifiers", "projects"));
		
		pubsData.add(header);

		int numPubs = 0;
		for (Publication pub : publications) {
			if (numPubs%1000 == 0) {
				System.out.println(numPubs);
			}
			
			List<String> pubData = new ArrayList<String>();
			
			pubData.add(Integer.toString(pub.getId()));
			pubData.add(pub.getTitle());
			pubData.add(pub.getSubtitle());
			pubData.add(pub.getLocationType());
			pubData.add(pub.getLocationName());
			if(pub.getPublicationDate() != null) {
				pubData.add(pub.getPublicationDate().toString());
			} else {
				pubData.add("");
			}
			pubData.add(pub.getDescription());
			pubData.add(pub.getUrl());
			pubData.add(pub.getType());
			
			// authors
			List<String> authors = new ArrayList<String>();
			for (Person person : pub.getAuthors()) {
				authors.add(person.getFirstName() + " " + person.getLastName());						
			}			
			pubData.add(String.join(";", authors));	

			// thematics
			List<String> themes = new ArrayList<String>();
			for (Thematic t : pub.getThematics()) {
				themes.add(t.getLabel());					
			}			
			pubData.add(String.join(";", themes));
			
			// publication identifiers
			List<String> pubIds = new ArrayList<String>();
			for (PublicationIdentifier pubId : pub.getPublicationIdentifiers()) {
				String identifier = pubId.getIdentifierName().replace(" ", ""); 
				pubIds.add(identifier + "_" + pubId.getIdentifier());						
			}			
			pubData.add(String.join(";", pubIds));
			
			// projects
			List<String> pubPrjs = new ArrayList<String>();
			for (Project pubPrj : pub.getProjects()) {
				pubPrjs.add(pubPrj.getLabel());						
			}			
			pubData.add(String.join(";", pubPrjs));

			pubsData.add(pubData);
			numPubs++;

		}
		
		return pubsData;

	}
	
	public static List<List<String>> exportLeaders(List<Leader> leaders, List<Organization> sourceOrgs) {

		List<List<String>> peopleData = new ArrayList<List<String>>();
		
		List<String> header = new ArrayList<>(Arrays.asList("firstname", "lastname", "title", "email",
				"orgs"));
		
		peopleData.add(header);	
		
		Map<Integer, List<Organization>> orgPersonMap = new HashMap<>();
		if (leaders.size() > 0) {
			// create org-projects map			
			for (Organization org: sourceOrgs) {
				List<Leader> orgsPeople = org.getLeaders();
				if (orgsPeople != null) {
					for (Leader per: orgsPeople) {
						int key = per.getId();
						if (orgPersonMap.get(key) != null) {
							orgPersonMap.get(key).add(org);
						} else {
							List<Organization> orgs = new ArrayList<>();
							orgs.add(org);
							orgPersonMap.put(key, orgs);
						}
					}
				}
			}
		}

		int numPeople = 0;
		for (Leader leader: leaders) {
			/*if (numPeople%1000 == 0) {
				System.out.println(numPeople);
			}*/
			
			List<String> personData = new ArrayList<String>();
			
			personData.add(leader.getFirstName());
			personData.add(leader.getLastName());
			personData.add(leader.getTitle());
			personData.add(leader.getEmail());

			// organizations
			//List<Organization> organizations = getOrgsFromPerson(sourceOrgs, person);
			List<Organization> organizations = orgPersonMap.get(leader.getId());
			if (organizations != null) {
				List<String> orgs = new ArrayList<String>();
				for (Organization org : organizations) {
					orgs.add(org.getId() + "_" + org.getLabel());					
				}
				personData.add(String.join(";", orgs));
			} else {
				personData.add("");
			}			
						
			peopleData.add(personData);
			numPeople++;

		}
		
		return peopleData;

	}
	
	public static List<List<String>> exportPeople(List<Person> people, EntityManager em, Source source, List<Organization> sourceOrgs) {

		List<List<String>> peopleData = new ArrayList<List<String>>();
		
		List<String> header = new ArrayList<>(Arrays.asList("id", "firstname", "lastname", "title", "email",
				"orgs", "identifiers"));
		
		peopleData.add(header);	
		
		Map<Integer, List<Organization>> orgPersonMap = new HashMap<>();
		if (people.size() > 0) {
			// create org-projects map			
			for (Organization org: sourceOrgs) {
				List<Person> orgsPeople = org.getPeople();
				if (orgsPeople != null) {
					for (Person per: orgsPeople) {
						int key = per.getId();
						if (orgPersonMap.get(key) != null) {
							orgPersonMap.get(key).add(org);
						} else {
							List<Organization> orgs = new ArrayList<>();
							orgs.add(org);
							orgPersonMap.put(key, orgs);
						}
					}
				}
			}
		}

		int numPeople = 0;
		for (Person person : people) {
			if (numPeople%1000 == 0) {
				System.out.println(numPeople);
			}
			
			List<String> personData = new ArrayList<String>();
			
			personData.add(Integer.toString(person.getId()));
			personData.add(person.getFirstName());
			personData.add(person.getLastName());
			personData.add(person.getTitle());
			personData.add(person.getEmail());

			// organizations
			//List<Organization> organizations = getOrgsFromPerson(sourceOrgs, person);
			List<Organization> organizations = orgPersonMap.get(person.getId());
			if (organizations != null) {
				List<String> orgs = new ArrayList<String>();
				for (Organization org : organizations) {
					orgs.add(org.getId() + "_" + org.getLabel());					
				}			
				personData.add(String.join(";", orgs));
			} else {
				personData.add("");
			}
			
			// person identifiers
			List<String> personIds = new ArrayList<String>();
			for (PersonIdentifier personId : person.getPersonIdentifiers()) {
				if (personId != null) {
					if (personId.getIdentifierName() != null) {
						String identifier = personId.getIdentifierName().replace(" ", "");
						personIds.add(identifier + "_" + personId.getIdentifier());
					}
				}
										
			}			
			personData.add(String.join(";", personIds));
						
			peopleData.add(personData);
			numPeople++;

		}
		
		return peopleData;

	}
	
	private static List<Organization> getOrgsFromProject(List<Organization> allOrgs, Project prj) {
		
		List<Organization> results = new ArrayList<>();
		
		for (Organization org: allOrgs) {
			List<Project> orgsProjects = org.getProjects();
			if (orgsProjects != null) {
				if (orgsProjects.contains(prj)) {
					results.add(org);
				}
			}
		}		

		return results;
	}
	
	private static void saveThemes(Project prj, List<String> prjData) {
		
		List<String> themes = new ArrayList<String>();

		for (Thematic t : prj.getThematics()) {
			themes.add(t.getLabel());					
		}
		
		prjData.add(String.join(";", themes));	
		
	}
	
	private static List<Publication> getPubsFromProject(List<Publication> allPubs, Project prj) {
		
		List<Publication> results = new ArrayList<>();
		
		for (Publication pub: allPubs) {
			List<Project> pubsProjects = pub.getProjects();
			if (pubsProjects != null) {
				if (pubsProjects.contains(prj)) {
					results.add(pub);
				}
			}
		}

		return results;
	}
	
	private static List<Organization> getOrgsFromPerson(List<Organization> allOrgs, Person p) {
		
		List<Organization> results = new ArrayList<>();
		
		for (Organization org: allOrgs) {
			List<Person> orgsPeople = org.getPeople();
			if (orgsPeople != null) {
				if (orgsPeople.contains(org)) {
					results.add(org);
				}
			}
		}		

		return results;
	}	

}
