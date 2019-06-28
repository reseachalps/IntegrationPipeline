package it.unimore.alps.exporter;

import java.util.ArrayList;
import java.util.Arrays;
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

import com.github.cliftonlabs.json_simple.JsonArray;
import com.github.cliftonlabs.json_simple.JsonObject;

import it.unimore.alps.sql.model.Badge;
import it.unimore.alps.sql.model.Leader;
import it.unimore.alps.sql.model.Link;
import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationActivity;
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
import it.unimore.alps.sql.model.Theme;
import it.unimore.alps.utils.CSVFileWriter;

public class ExporterCSV {

	public static void main(String[] args) {
		
		List<String> sourceNames = new ArrayList<String>(Arrays.asList("All", "Arianna - Anagrafe Nazionale delle Ricerche", "Questio", "OpenAire", "CercaUniversita", "Bureau van Dijk", "Consiglio Nazionale delle Ricerche (CNR)", "ScanR", "ORCID", "Patiris", "Startup - Registro delle imprese"));
		
		CommandLine commandLine;
        Option sourceNameOption = Option.builder("sourceName")
        		.hasArg()
        		.required(true)
        		.desc("The source to be exported. The possible choice are: All, Arianna - Anagrafe Nazionale delle Ricerche, Questio, OpenAire, CercaUniversita, Bureau van Dijk, Consiglio Nazionale delle Ricerche (CNR), ScanR, ORCID, Patiris and Startup - Registro delle imprese.")
        		.longOpt("sourceName")
        		.build();                

        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addOption(sourceNameOption);
        
        String sourceName = null;
                	
        try {
			commandLine = parser.parse(options, args);
		
			System.out.println("----------------------------");
			System.out.println("OPTIONS:");
			
	        if (commandLine.hasOption("sourceName")) {   
	        	sourceName = commandLine.getOptionValue("sourceName");
	        	
	        	if (!sourceNames.contains(sourceName)) {
	        		System.out.println("\tWrong source name. The possible choice are: All, Arianna - Anagrafe Nazionale delle Ricerche, Questio, OpenAire, CercaUniversita, Bureau van Dijk, Consiglio Nazionale delle Ricerche (CNR), ScanR, ORCID, Patiris and Startup - Registro delle imprese.");
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
			
			System.out.println("----------------------------\n");
        
        } catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
        
        // -sourceName <sourcename_or_all>
        
		
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory("alpsv2");
        //EntityManagerFactory emfactory = Persistence.createEntityManagerFactory("scanr");
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
        
        for(Source source: sources) {
     	      	
        	// export organizations
        	List<Organization> orgs = retrieveOrganizations(entitymanager, source);        	            
        	List<List<String>> exportedOrganization = exportOrganizations(orgs);            
            //CSVFileWriter writer = new CSVFileWriter("/home/matteo/Scrivania/organizations_" + source.getLabel() + ".csv");
        	CSVFileWriter writer = new CSVFileWriter("/home/matteop/organizations_" + source.getLabel() + ".csv");
            writer.write(exportedOrganization);
            
            // export projects
            List<Project> prjs = retrieveProjects(entitymanager, source);
            List<List<String>> exportedProjects = exportProjects(prjs, entitymanager, source);             
            //writer = new CSVFileWriter("/home/matteo/Scrivania/projects_" + source.getLabel() + ".csv");
            writer = new CSVFileWriter("/home/matteop/projects_" + source.getLabel() + ".csv");
            writer.write(exportedProjects);   
            
            /*// export publications
            List<Publication> pubs = retrievePublications(entitymanager, source);
            List<List<String>> exportedPublications = exportPublications(pubs, entitymanager);             
            //writer = new CSVFileWriter("/home/matteo/Scrivania/publications_" + source.getLabel() + ".csv");
            writer = new CSVFileWriter("/home/matteop/publications_" + source.getLabel() + ".csv");
            writer.write(exportedPublications);
            
            // export people
            List<Person> people = retrievePeople(entitymanager, source);
            List<List<String>> exportedPeople = exportPeople(people, entitymanager, source);             
            //writer = new CSVFileWriter("/home/matteo/Scrivania/people_" + source.getLabel() + ".csv");
            writer = new CSVFileWriter("/home/matteop/people_" + source.getLabel() + ".csv");
            writer.write(exportedPeople);*/
        	
        }
		
		entitymanager.close();
		emfactory.close();
		
	}
	
	public static List<Organization> retrieveOrganizations(EntityManager entitymanager, Source source) {
		
		entitymanager.getTransaction().begin();
    	System.out.println("Starting retrieving organizations deriving from " + source.getLabel() + " source");
        Query query_org = entitymanager.createQuery("Select o FROM Organization o WHERE :id MEMBER OF o.sources");
        query_org.setParameter("id", source);
        List<Organization> orgs = query_org.getResultList();
        
        entitymanager.getTransaction().commit();
        System.out.println("Retrieved " + orgs.size() + " organizations deriving from " + source.getLabel() + " source");
        
        return orgs;
	}
	
	public static List<Project> retrieveProjects(EntityManager entitymanager, Source source) {
		
		entitymanager.getTransaction().begin();
        System.out.println("Starting retrieving projects deriving from " + source.getLabel() + " source");
        Query query_prj = entitymanager.createQuery("Select o FROM Project o WHERE :id MEMBER OF o.sources");
        query_prj.setParameter("id", source);
        //Query query_prj = entitymanager.createQuery("Select o FROM Project o");        
        List<Project> prjs = query_prj.getResultList();
        
        entitymanager.getTransaction().commit();
        System.out.println("Retrieved " + prjs.size() + " projects deriving from " + source.getLabel() + " source");
        
        return prjs;
	}
	
	public static List<Publication> retrievePublications(EntityManager entitymanager, Source source) {
		
		entitymanager.getTransaction().begin();
        System.out.println("Starting retrieving publications deriving from " + source.getLabel() + " source");
        Query query_prj = entitymanager.createQuery("Select o FROM Publication o WHERE :id MEMBER OF o.sources");
        query_prj.setParameter("id", source);
        //Query query_prj = entitymanager.createQuery("Select o FROM Project o");        
        List<Publication> pubs = query_prj.getResultList();
        
        entitymanager.getTransaction().commit();
        System.out.println("Retrieved " + pubs.size() + " publications deriving from " + source.getLabel() + " source");
        
        return pubs;
	}
	
	public static List<Person> retrievePeople(EntityManager entitymanager, Source source) {
		
		entitymanager.getTransaction().begin();
        System.out.println("Starting retrieving people deriving from " + source.getLabel() + " source");
        Query query_prj = entitymanager.createQuery("Select o FROM Person o WHERE :id MEMBER OF o.sources");
        query_prj.setParameter("id", source);
        //Query query_prj = entitymanager.createQuery("Select o FROM Project o");        
        List<Person> people = query_prj.getResultList();
        
        entitymanager.getTransaction().commit();
        System.out.println("Retrieved " + people.size() + " people deriving from " + source.getLabel() + " source");
        
        return people;
	}
	
	
	public static List<List<String>> exportOrganizations(List<Organization> organizations) {

		List<List<String>> orgsData = new ArrayList<List<String>>();
		
		List<String> header = new ArrayList<>(Arrays.asList("id", "acronyms", "alias", "label", "creationYear",
				"commercialLabel", "address", "city", "citycode", "country", "countryCode", "postcode", "urbanUnit",
				"urbanUnitCode", "lat", "lon", "revenueRange", "privateFinanceDate", "employees", "typeCategoryCode",
				"typeLabel", "typeKind", "isPublic", "leaders", "links", "privateOrgTypeId", "privateOrgTypeLabel",
				"activities", "relations", "badges", "children", "identifiers"));
		
		orgsData.add(header);

		int numOrgs = 0;
		for (Organization org : organizations) {
			if (numOrgs%1000 == 0) {
				System.out.println(numOrgs);
			}
			
			List<String> orgData = new ArrayList<String>();
			
			orgData.add(Integer.toString(org.getId()));
			//orgData.add(org.getAcronym);
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

			orgsData.add(orgData);
			numOrgs++;

		}
		
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
		orgData.add(Float.toString(org.getLat()));
		orgData.add(Float.toString(org.getLon()));						

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
			activities.add(act.getCode());// + "_" + act.getLabel());
		}
		
		orgData.add(String.join(";", activities));
		
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

		for (Person person : org.getPeople()) {
			leaders.add(person.getFirstName() + " " + person.getLastName());						
		}
		
		orgData.add(String.join(";", leaders));		

	}
	
	private static void saveAcronyms(Organization org, List<String> orgData) {
		
		List<String> acronyms = new ArrayList<String>();

		for (String acronym: org.getAcronyms()) {
			acronyms.add(acronym);						
		}
		
		orgData.add(String.join(";", acronyms));		

	}
	
	private static void saveOrganizationChildren(Organization org, List<String> orgData) {
		
		List<String> children = new ArrayList<String>();

		for (Organization orgC : org.getChildrenOrganizations()) {
			children.add(orgC.getId() + "_" + orgC.getLabel());						
		}
		
		orgData.add(String.join(";", children));	
		
	}
	
	public static List<List<String>> exportProjects(List<Project> projects, EntityManager em, Source source) {
		
		List<List<String>> prjsData = new ArrayList<List<String>>();
		
		List<String> header = new ArrayList<>(Arrays.asList("id", "acronym", "label", "description", "duration",
				"budget", "call", "callLabel", "url", "startDate", "type", "year", "month", "themes", "orgs", "pubs", "identifiers"));
		
		prjsData.add(header);		
		
		int numPrj = 0;

		for (Project prj : projects) {
			
			if (numPrj%1000 == 0) {
				System.out.println(numPrj);
			}
			
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
			List<Organization> organizations = getOrgsFromProject(prj, em, source);			
			List<String> orgs = new ArrayList<String>();
			for (Organization org : organizations) {
				orgs.add(org.getId() + "_" + org.getLabel());					
			}			
			prjData.add(String.join(";", orgs));
			
			// publications
			List<Publication> publications = getPubsFromProject(prj, em, source);			
			List<String> pubs = new ArrayList<String>();
			for (Publication pub: publications) {
				pubs.add(pub.getId() + "_" + pub.getTitle());					
			}			
			prjData.add(String.join(";", pubs));
			
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
	
	public static List<List<String>> exportPeople(List<Person> people, EntityManager em, Source source) {

		List<List<String>> peopleData = new ArrayList<List<String>>();
		
		List<String> header = new ArrayList<>(Arrays.asList("id", "firstname", "lastname", "title", "email",
				"orgs", "identifiers"));
		
		peopleData.add(header);

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
			List<Organization> organizations = getOrgsFromPerson(person, em, source);			
			List<String> orgs = new ArrayList<String>();
			for (Organization org : organizations) {
				orgs.add(org.getId() + "_" + org.getLabel());					
			}			
			personData.add(String.join(";", orgs));
			
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
	
	private static List<Organization> getOrgsFromProject(Project prj, EntityManager entitymanager, Source source) {
		
		entitymanager.getTransaction().begin();
		Query query = entitymanager.createQuery("SELECT o FROM Organization o WHERE :id MEMBER OF o.sources and :prj MEMBER OF o.projects");
		query.setParameter("prj", prj);
		query.setParameter("id", source);
		entitymanager.getTransaction().commit();

		List<Organization> results = query.getResultList();

		return results;
	}
	
	private static void saveThemes(Project prj, List<String> prjData) {
		
		List<String> themes = new ArrayList<String>();

		for (Thematic t : prj.getThematics()) {
			themes.add(t.getLabel());					
		}
		
		prjData.add(String.join(";", themes));	
		
	}
	
	private static List<Publication> getPubsFromProject(Project prj, EntityManager entitymanager, Source source) {
		Query query = entitymanager.createQuery("SELECT p FROM Publication p WHERE :id MEMBER OF p.sources and :prj MEMBER OF p.projects");
		query.setParameter("prj", prj);
		query.setParameter("id", source);
		List<Publication> results = query.getResultList();

		return results;
	}
	
	private static List<Organization> getOrgsFromPerson(Person p, EntityManager entitymanager, Source source) {
		Query query = entitymanager.createQuery("SELECT o FROM Organization o WHERE :id MEMBER OF o.sources and :p MEMBER OF o.people");
		query.setParameter("p", p);
		query.setParameter("id", source);
		List<Organization> results = query.getResultList();

		return results;
	}	

}
