package it.unimore.alps.sources.arianna;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.opencsv.CSVReader;

import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationExtraField;
import it.unimore.alps.sql.model.OrganizationIdentifier;
import it.unimore.alps.sql.model.OrganizationType;
import it.unimore.alps.sql.model.Project;
import it.unimore.alps.sql.model.ProjectExtraField;
import it.unimore.alps.sql.model.ProjectIdentifier;
import it.unimore.alps.sql.model.Source;

public class AriannaImporter {
	
	private String orgFile;
	private String projectFile;
	private String bindingFile;
	private CSVReader orgReader;
	private CSVReader projectReader;
	private CSVReader bindingReader;
	List<String> orgSchema = new ArrayList<String>();
	List<String> projectSchema = new ArrayList<String>();
	List<String> bindingSchema = new ArrayList<String>();
	List<Map<String, String>> orgData = new ArrayList<>();
	List<Map<String, String>> projectData = new ArrayList<>(); 
	private String sourceName = "Arianna - Anagrafe Nazionale delle Ricerche";
	private String sourceUrl = "http://www.anagrafenazionalericerche.it/arianna/contentpages/default.aspx";
	private String sourceRevisionDate = "15-12-2017";
	
	
	public static void main(String[] args) {
		
		CommandLine commandLine;
        Option orgFileOption = Option.builder("orgFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains organization data. ")
	            .longOpt("organizationFile")
	            .build();
        Option projectFileOption = Option.builder("prjFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains project data.")
	            .longOpt("projectFile")
	            .build();
        Option orgPrjFileOption = Option.builder("orgPrjFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains the links between organizations and projects.")
	            .longOpt("organizationProjectFile")
	            .build();
        Option entityTypeOption = Option.builder("entityType")
        		.hasArg()
	            .required(true)
	            .desc("The entity type to be imported. The possibile choiches are: 'all', 'organization', 'project' and 'link'.")
	            .longOpt("entityTypeImported")
	            .build();
        Option incrementalModeOption = Option.builder("incMode")
	            .required(false)
	            .desc("The incremental mode consists in importing only new data.")
	            .longOpt("incrementalMode")
	            .build();
        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addOption(orgFileOption);
        options.addOption(projectFileOption);
        options.addOption(orgPrjFileOption);
        options.addOption(entityTypeOption);
        options.addOption(incrementalModeOption);
        
        String orgFile = null;
        String projectFile = null;
        String bindingFile = null;
        String entityType = null;
        boolean incrementalMode = false;
        boolean header = true;
                	
        try {
			commandLine = parser.parse(options, args);
						
			System.out.println("----------------------------");
			System.out.println("OPTIONS:");

	        if (commandLine.hasOption("orgFile")) {   
	        	orgFile = commandLine.getOptionValue("orgFile");
	        	System.out.println("\tOrganization file: " + orgFile);
			} else {
				System.out.println("\tOrganization file not provided. Use the orgFile option.");
	        	System.exit(1);
			}
			
			if (commandLine.hasOption("prjFile")) {
				projectFile = commandLine.getOptionValue("prjFile");
				System.out.println("\tProject file: " + projectFile);
			} else {
				System.out.println("\tProject file not provided. Use the prjFile option.");
	        	System.exit(1);
			}
	
			if (commandLine.hasOption("orgPrjFile")) {
				bindingFile = commandLine.getOptionValue("orgPrjFile");
				System.out.println("\tBinding file: " + bindingFile);
			} else {
				System.out.println("\tFile containing organization-project links not provided. Use the orgPrjFile option.");
				System.exit(1);
			}  					
			
			if (commandLine.hasOption("entityType")) {
				entityType = commandLine.getOptionValue("entityType");
				System.out.println("\tEntity type to be exported: " + entityType);
				
				if (!entityType.equals("all") && !entityType.equals("organization") && !entityType.equals("project") && !entityType.equals("link")) {
					System.out.println("\tWrong entity type value provided. Only the following entity type values are allowed: 'all', 'organization', 'project' and 'link'.");
					System.exit(1);
				}
				
			} else {
				System.out.println("\tEntity type to be imported not provided. Use the entityType option.");
				System.exit(1);
			}
			
			if (commandLine.hasOption("incMode")) {
				System.out.println("\tIncremental mode activated");
				incrementalMode = true;
			}
			
			System.out.println("----------------------------\n");
        
        } catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        //String orgFile = "/media/matteo/Windows/Users/matte/Dropbox/Matteo/Documenti/Studio/Progetto_RESEARCH_ALPS/Dataset/Arianna/arianna1_soggetti_01dic17.csv";
		//String projectFile = "/media/matteo/Windows/Users/matte/Dropbox/Matteo/Documenti/Studio/Progetto_RESEARCH_ALPS/Dataset/Arianna/arianna_progetti_01dic17.csv";
		//String bindingFile = "/media/matteo/Windows/Users/matte/Dropbox/Matteo/Documenti/Studio/Progetto_RESEARCH_ALPS/Dataset/Arianna/raccordo soggetti progetti_19dic2017.csv";
		//AriannaImporter ariannaImporter = new AriannaImporter(orgFile, projectFile, bindingFile, header);
		//ariannaImporter.importData("all");
		//ariannaImporter.importData("organization");
		//ariannaImporter.importData("project");
		//ariannaImporter.importData("link");
		
        AriannaImporter ariannaImporter = new AriannaImporter(orgFile, projectFile, bindingFile, header);
        ariannaImporter.importData(entityType, incrementalMode);

	}
	
	private CSVReader initializeCSVReader(String inputFile, boolean header, List<String> schema) {
		
		CSVReader reader = null;
		
		try {
			reader = new CSVReader(new FileReader(inputFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		// extract the header from input file
		if (reader != null) {
			if (header == true) {
				String[] line;
				try {
					if ((line = reader.readNext()) != null) {
						for(int i=0; i<line.length; i++)
							schema.add(line[i]);	
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		return reader;
	}
	
	public AriannaImporter(String orgFile, String projectFile, String bindingFile, boolean header) {
		this.orgFile = orgFile;
		this.projectFile = projectFile;
		this.bindingFile = bindingFile;
		
		this.orgReader = initializeCSVReader(orgFile, header, orgSchema);
		this.projectReader = initializeCSVReader(projectFile, header, projectSchema);
		this.bindingReader = initializeCSVReader(bindingFile, header, bindingSchema);
	}
	
	private Source setSource() {
		Source source = new Source();
		source.setLabel(sourceName);
		source.setUrl(this.sourceUrl);
		DateFormat df = new SimpleDateFormat("dd-MM-yyyy");
        
		Date sourceDate;
		try {
			sourceDate = df.parse(sourceRevisionDate);
			source.setRevisionDate(sourceDate);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return source;		
	}
	
	private List<Map<String, String>> createMapFromFileCSV(CSVReader reader, List<String> schema) {
		
		List<Map<String, String>> fullData = new ArrayList<Map<String, String>>();
		
		// read data and store it in a map
		String[] line;
		//List<Map<String, String>> fullData = new ArrayList<Map<String, String>>();
        try {
            while ((line = reader.readNext()) != null) {
            	Map<String, String> rowData = new HashMap<String, String>();
	        	for(int i=0; i< line.length; i++) {
	        		String key = schema.get(i);
	        		String value = line[i];
	        		rowData.put(key, value.toString());
	        	}
	        	/*for(Object objname: row_data.keySet()) {
	     		   System.out.println(objname);
	     		   System.out.println(row_data.get(objname));
	     		 }*/
	        	fullData.add(rowData);
	        	
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return fullData;
        
	}
	
	private Organization setOrganizationFields(Map<String,String> data, List<Source> sources) {
		
		// ipotizzo che le organizzazioni fornite nel file csv siano uniche
		
		Organization org = new Organization();
		
    	String label = data.get("DENOMINAZIONE");
		if (!label.equals("")) {
			org.setLabel(label);
		}
	    
	    String address = data.get("INDIRIZZO");
	    if (!address.equals("")) {
	    	org.setAddress(address);
	    	org.setAddressSources(sources);
	    }
	    
	    String cap = data.get("CAP");
	    if (!cap.equals("")) {
	    	org.setPostcode(cap);
	    }
	    
	    String city = data.get("COMUNE");
	    if (!city.equals("")) {
	    	org.setCity(city);
	    }
	    
	    String urbanUnit = data.get("PROVINCIA");
	    if (!urbanUnit.equals("")) {
	    	org.setUrbanUnit(urbanUnit);
	    }
	    
	    org.setCountry("Italy");
	    org.setCountryCode("IT");
	    
	    String capSoc = data.get("CAPITALE SOCIALE");
	    if (!capSoc.equals("")) {
	    	if (capSoc.equals("Capitale prevalentemente pubblico")) {
	    		org.setIsPublic("true");
	    	} else {
	    		if (capSoc.equals("Capitale prevalentemente privato italiano")) {
		    		org.setIsPublic("false");
		    	} else { // Non applicabile o vuoto
		    		String natGiur = data.get("NATURA GIURIDICA");
		    		if (!natGiur.equals("")) {
		    			if (natGiur.equals("Altre societa' private")) {
		    				org.setIsPublic("false");
		    			}
		    			if (natGiur.equals("Altri Enti")) {
		    				org.setIsPublic("true");
		    			}
		    			if (natGiur.equals("Aziende provinciali, regionali, comunali e loro consorzi")) {
		    				org.setIsPublic("true");
		    			}
		    			if (natGiur.equals("Cooperative")) {
		    				org.setIsPublic("false");
		    			}
		    			if (natGiur.equals("Enti Pubblici economici")) {
		    				org.setIsPublic("true");
		    			}
		    			if (natGiur.equals("Enti Pubblici non economici")) {
		    				org.setIsPublic("true");
		    			}
		    			if (natGiur.equals("Fondazioni")) {
		    				org.setIsPublic("false");
		    			}
		    			if (natGiur.startsWith("Societa")) {
		    				org.setIsPublic("false");
		    			}
		    		}
		    	}
	    	
	    	}
	    }
	    
	    return org;
		
	}
	
	private List<OrganizationIdentifier> setOrganizationIdentifiers(Map<String,String> data, Organization org) {
	  	 
	  	List<OrganizationIdentifier> orgIds = new ArrayList<OrganizationIdentifier>();
	  	
	  	OrganizationIdentifier orgId1 = null;
	  	String orgIdentifier = data.get("CODICE FISCALE");
	  	if (!orgIdentifier.equals("")) {
	  		orgId1 = new OrganizationIdentifier();
	  		orgId1.setIdentifier(orgIdentifier);
	  		orgId1.setProvenance(sourceName); 		
	  		orgId1.setIdentifierName("CODICE FISCALE");
	  		orgId1.setOrganization(org);
	  		orgIds.add(orgId1);
	  	}
	  	
	  	OrganizationIdentifier orgId2 = null;
	  	orgIdentifier = data.get("CAR");
	  	if (!orgIdentifier.equals("")) {
	  		orgId2 = new OrganizationIdentifier();
	  		orgId2.setIdentifier(orgIdentifier); 
	  		orgId2.setProvenance(sourceName); 	  		
	  		orgId2.setIdentifierName("CAR");
	  		orgId2.setOrganization(org);
	  		orgIds.add(orgId2);
	  	}
	  	
	  	return orgIds;
	  	 
	}
	
	private OrganizationType setOrganizationType(Map<String,String> data, EntityManager em) {
			
		OrganizationType orgType = null;
		
		String type = data.get("NATURA GIURIDICA");
		if(!type.equals("")) {
			
			Query query = em.createQuery("Select e FROM OrganizationType e WHERE e.label = :label");
	        query.setParameter("label", type);
	        List<OrganizationType> results = query.getResultList();
	        //System.out.println("******************");
	        //for (OrganizationType res: results) {
	        //	System.out.println(res.getLabel());
	        //}
	        //System.out.println("******************");
	        if (results.size() > 0) {
	        	if (results.get(0) != null) {
	        		orgType = results.get(0);
	        		//System.out.println("ESISTE");
	        	}
	        } else {
	        	orgType = new OrganizationType();
	        	orgType.setLabel(type);
	        	//System.out.println("NON ESISTE");
	        	em.persist(orgType);
	        }
			//orgType = new OrganizationType();
        	//orgType.setLabel(type);
		}
		
		return orgType;
	}
	
	private List<OrganizationExtraField> setOrganizationExtraFields(Map<String,String> data, Set<String> attributes, Organization org) {
		List<OrganizationExtraField> orgExtraFields = new ArrayList<OrganizationExtraField>();
		
		Set<String> visibleAttributes = new HashSet<String>(Arrays.asList("REGIONE", "PREFISSO", "TELEFONO", "FAX", "EMAIL", "CAPITALE SOCIALE"));
		
		for (String key : data.keySet()) {
			if (!attributes.contains(key)) {
				if (!data.get(key).equals("")) {
					OrganizationExtraField orgExtraField = new OrganizationExtraField();
					if (visibleAttributes.contains(key)) {
						orgExtraField.setVisibility(true);
					}
					orgExtraField.setFieldKey(key);
					orgExtraField.setFieldValue(data.get(key));
					orgExtraField.setOrganization(org);
					orgExtraFields.add(orgExtraField);
				}
			}
		}
		
		return orgExtraFields;
	}
	
	private Project setProjectFields(Map<String,String> data) {
		 	 
		Project project = new Project();
		
    	String label = data.get("TITOLO PROGETTO");
		if (!label.equals("")) {
			project.setLabel(label);
		}
	    
	    String descr = data.get("SINTESI PROGETTO");
	    if (!descr.equals("")) {
	    	project.setDescription(descr);
	    }
	    
	    String startDateString = data.get("DATA INIZIO");
	    if (!startDateString.equals("")) {
	    	
	    	DateFormat df = new SimpleDateFormat("dd/MM/yyyy");
	        
			try {
				Date startDate = df.parse(startDateString);
				Calendar cal = Calendar.getInstance();
				cal.setTime(startDate);
				project.setStartDate(startDate);
				project.setYear(Integer.toString(cal.get(Calendar.YEAR)));
				project.setMonth(Integer.toString(cal.get(Calendar.MONTH)));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	    
	    String monthDurationString = data.get("DURATA MESI");
	    if (!monthDurationString.equals("")) {
	    	int monthDuration = Integer.parseInt(monthDurationString); 
	    	//int dayDuration = monthDuration * 31;
	    	int dayDuration = monthDuration;
	    	String dayDurationString = Integer.toString(dayDuration);
	    	project.setDuration(dayDurationString);
	    }
	    
	    String budget = data.get("COSTI AMMESSI");
	    if (!budget.equals("")) {
	    	project.setBudget(budget);
	    }
	    
	    return project;
	}
	
	private List<ProjectIdentifier> setProjectIdentifiers(Map<String,String> data, Project project) {
		
		List<ProjectIdentifier> projectIds = new ArrayList<ProjectIdentifier>();
	  	
		ProjectIdentifier projectId = null;
	  	String projectIdentifier = data.get("CUR");
	  	if (!projectIdentifier.equals("")) {
	  		projectId = new ProjectIdentifier();
	  		projectId.setIdentifier(projectIdentifier);
	  		projectId.setProvenance(sourceName);
	  		projectId.setIdentifierName("CUR");
	  		projectId.setProject(project);
	  		projectIds.add(projectId);
	  	}
	  	
	  	return projectIds;
	  	
	}
	
	private List<ProjectExtraField> setProjectExtraFields(Map<String,String> data, Set<String> attributes, Project project) {
		
		List<ProjectExtraField> projectExtraFields = new ArrayList<ProjectExtraField>();
		
		Set<String> visibleAttributes = new HashSet<String>(Arrays.asList("SETTORE", "PROVINCIA SVOLGIMENTO", "REGIONE SVOLGIMENTO", "DIMENSIONE IMPRESA", "COLLABORAZ PARTNER UE", "COLLABORAZ ENTI PUB RICERCA"));
		
		for (String key : data.keySet()) {
			if (!attributes.contains(key)) {
				if (!data.get(key).equals("")) {
					ProjectExtraField projectExtraField = new ProjectExtraField();
					if (visibleAttributes.contains(key)) {
						projectExtraField.setVisibility(true);
					}
					projectExtraField.setFieldKey(key);
					projectExtraField.setFieldValue(data.get(key));
					projectExtraField.setProject(project);
					projectExtraFields.add(projectExtraField);
				}
			}
		}
		
		return projectExtraFields;
	}
	
	private Map<String,Organization> importOrganizations(EntityManager entitymanager, List<Source> sources, boolean incrementalMode) {
		
		this.orgData = createMapFromFileCSV(orgReader, orgSchema);
		
		// read organization data and import it in the classes of the database 
		System.out.println("Starting importing organizations...");
		////////////entitymanager.getTransaction().begin();
        Map<String,Organization> orgMap = new HashMap<String,Organization>(); 
        Set<String> attributes = new HashSet<String>(Arrays.asList("CODICE FISCALE", "CAR", "DENOMINAZIONE", "INDIRIZZO", "CAP", "COMUNE", "PROVINCIA", "NATURA GIURIDICA"));
        boolean start = true;
        for(int i=0; i<orgData.size(); i++) {
        	if ((i % 1000) == 0) {
        		System.out.println(i);
        	}
        	
        	Map<String, String> rowData = orgData.get(i);
        		
        	/*entitymanager.getTransaction().begin();
            Query query_org = entitymanager.createQuery("Select o FROM Organization o WHERE :source MEMBER OF o.sources and o.label=:label");
            query_org.setParameter("source", sources.get(0));
    		if (!rowData.get("DENOMINAZIONE").equals("")) {
    			query_org.setParameter("label", rowData.get("DENOMINAZIONE"));
    		}
            List<Organization> orgs = query_org.getResultList();
            entitymanager.getTransaction().commit();*/
        	
        	if (incrementalMode == true) {
	        	entitymanager.getTransaction().begin();
	            Query query_org = entitymanager.createQuery("Select p FROM OrganizationIdentifier p WHERE p.identifierName=:idName and p.identifier=:id");
	            query_org.setParameter("idName", "CAR");
	            query_org.setParameter("id", rowData.get("CAR"));    		
	            List<OrganizationIdentifier> orgs = query_org.getResultList();
	            entitymanager.getTransaction().commit();
	            
	            if (orgs.size() > 0) {
	            	continue;          
	            } else {
	            	if (start == true) {
	            		System.out.println("Starting from index: " + i);
	            		start = false;
	            	}
	            }
        	}
            
            entitymanager.getTransaction().begin();
        	// set main organization fields
        	Organization org = setOrganizationFields(rowData, sources);
        	
        	// set organization types
        	OrganizationType orgType = setOrganizationType(rowData, entitymanager);
        	if (orgType != null) {
        		// connect organization type with related organization
        		org.setOrganizationType(orgType);
        	}
        	
        	// set organization identifiers
        	if (org != null) {
        		List<OrganizationIdentifier> orgIdentifiers = setOrganizationIdentifiers(rowData, org);
        		// create OrganizationIdentifier tuples in the DB
        		for (OrganizationIdentifier orgIdentifier: orgIdentifiers) {
        			entitymanager.persist(orgIdentifier);
        		}
        		// index organization with CAR code
        		String orgId = rowData.get("CAR");
            	if (!orgId.equals("")) {
            		orgMap.put(orgId, org);
            	}
        	}
        	
        	// connect the source to the related organization
        	if (org != null) {        		
        		org.setSources(sources);
        	}

        	// set organization extra fields
        	if (org != null) {
        		List<OrganizationExtraField> orgExtraFields = setOrganizationExtraFields(rowData, attributes, org);
        		for (OrganizationExtraField orgExtraField: orgExtraFields) {
        			entitymanager.persist(orgExtraField);
        		}
        	}
        	
        	if (org != null) {
        		entitymanager.persist(org);
        	}
        	
        	entitymanager.getTransaction().commit();
        }
        
        ////////////entitymanager.getTransaction().commit();
        
        System.out.println("Imported " + orgMap.size() + " organizations");
        
        return orgMap;
	}
	
	private Map<String,Project> importProjects(EntityManager entitymanager, List<Source> sources, boolean incrementalMode) {
		
		this.projectData = createMapFromFileCSV(projectReader, projectSchema);
		
		// read project data and import it in the classes of the database 
		System.out.println("Starting importing projects...");
		////////////////entitymanager.getTransaction().begin();
		Map<String,Project> projectMap = new HashMap<String,Project>(); 
		Set<String> attributes = new HashSet<String>(Arrays.asList("CUR", "TITOLO PROGETTO", "SINTESI PROGETTO", "DATA INIZIO", "DURATA MESI", "COSTI AMMESSI", "PARTECIPANTI"));
		boolean start = true;
        for(int j=0; j<projectData.size(); j++) {
	        if ((j % 1000) == 0) {
	        		System.out.println(j);
	        	}
        	Map<String, String> rowData = projectData.get(j);
        	
        	if (incrementalMode == true) {
	        	entitymanager.getTransaction().begin();
	            Query query_prj = entitymanager.createQuery("Select p FROM ProjectIdentifier p WHERE p.identifierName=:idName and p.identifier=:id");
	            query_prj.setParameter("idName", "CUR");
	            query_prj.setParameter("id", rowData.get("CUR"));    		
	            List<ProjectIdentifier> prjs = query_prj.getResultList();
	            entitymanager.getTransaction().commit();
	            
	            if (prjs.size() > 0) {
	            	continue;          
	            } else {
	            	if (start == true) {
	            		System.out.println("Starting from index: " + j);
	            		start = false;
	            	}
	            }
        	}
            
            entitymanager.getTransaction().begin();
        		
        	// set main project fields
        	Project project = setProjectFields(rowData);
        	      	
        	// set project identifiers
        	if (project != null) {
        		List<ProjectIdentifier> projectIdentifiers = setProjectIdentifiers(rowData, project);
        		// create ProjectIdentifier tuples in the DB
        		for (ProjectIdentifier projectIdentifier: projectIdentifiers) {
        			entitymanager.persist(projectIdentifier);
        		}
        		// index project with CUR code
        		String projectId = rowData.get("CUR");
            	if (!projectId.equals("")) {
            		projectMap.put(projectId, project);
            	}
        	}
        	
        	// connect the source to the related project
        	if (project != null) {
        		project.setSources(sources);
        	}
        	
        	// set project extra fields
        	if (project != null) {
        		List<ProjectExtraField> projectExtraFields = setProjectExtraFields(rowData, attributes, project);
        		for (ProjectExtraField projectExtraField: projectExtraFields) {
        			entitymanager.persist(projectExtraField);
        		}
        	}
        	
        	if (project != null) {
        		entitymanager.persist(project);
        	}        	
        	
        	entitymanager.getTransaction().commit();
        }
        
        ////////////////entitymanager.getTransaction().commit();
        
        System.out.println("Imported " + projectData.size() + " projects");
        
        return projectMap;
		
	}
	
	private Map<String,Organization> retrieveOrganizations(EntityManager entitymanager, Source source) {
		
		entitymanager.getTransaction().begin();
		
		// retrieve organizations
		System.out.println("Starting retrieving organizations...");
        Query query_org = entitymanager.createQuery("Select o FROM Organization o WHERE :id MEMBER OF o.sources");
        query_org.setParameter("id", source);
        List<Organization> orgs = query_org.getResultList();
        
        Map<String,Organization> orgMap = new HashMap<String,Organization>();
        int num_org = 0;
        for(Organization org: orgs) {
        	if ((num_org % 1000) == 0) {
        		System.out.println(num_org);
        	}
        	List<OrganizationIdentifier> orgIds = org.getOrganizationIdentifiers();
        	String CAR = null;
        	for(OrganizationIdentifier orgId: orgIds) {
        		if (orgId.getIdentifierName().equals("CAR")) { 
        			CAR = orgId.getIdentifier();
        			break;
        		}
        	}
        	orgMap.put(CAR, org);
        	num_org += 1;
        }
        
        System.out.println("Retrieved " + orgMap.size() + " organizations");
        entitymanager.getTransaction().commit();
        
        return orgMap;
	}
	
	private Map<String,Project> retrieveProjects(EntityManager entitymanager, Source source) {
		
		entitymanager.getTransaction().begin();
		
		// retireve projects
        System.out.println("Starting retrieving projects...");        
        Query query_project = entitymanager.createQuery("Select p FROM Project p WHERE :id MEMBER OF p.sources");
        query_project.setParameter("id", source);
        List<Project> prjs = query_project.getResultList();
        
        Map<String,Project> projectMap = new HashMap<String,Project>();
        int num_projects = 0;
        for(Project prj: prjs) {
        	if ((num_projects % 100) == 0) {
        		System.out.println(num_projects);
        	}
        	List<ProjectIdentifier> prjIds = prj.getProjectIdentifiers();
        	String CUR = prjIds.get(0).getIdentifier();
        	projectMap.put(CUR, prj);
        	num_projects += 1;
        }
        
        System.out.println("Retrieved " + projectMap.size() + " projects");
        entitymanager.getTransaction().commit();
        
        return projectMap;
	}
	
	private void importOrganizationProjectLinks(EntityManager entitymanager, Map<String,Organization> orgMap, Map<String,Project> projectMap) {		    
        
		// creating binding map ---------------------------------------------------
		String[] line;		
		Map<String, List<String>> bindingMap = new HashMap<String, List<String>>();
        try {
            while ((line = bindingReader.readNext()) != null) {
            	
            	List<String> value = bindingMap.get(line[1]);
            	if (value != null) {
            		bindingMap.get(line[1]).add(line[0]);
            	} else {
            		List<String> val = new ArrayList<String>();
            		val.add(line[0]);
            		bindingMap.put(line[1], val);
            	}
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
		// ------------------------------------------------------------------------ 
                                     
        // connect projects with related organization
        //////////////entitymanager.getTransaction().begin();
        int num_bind = 0;
        for (String orgKey : bindingMap.keySet()) {
        	entitymanager.getTransaction().begin();
        	if ((num_bind % 100) == 0) {
        		System.out.println(num_bind);
        	}
            Organization org = orgMap.get(orgKey);
            List<String> projectKeys = bindingMap.get(orgKey);
            List<Project> projects = new ArrayList<Project>();
            if (projectKeys != null) {
	            for(String projectKey: projectKeys) {
	            	projects.add(projectMap.get(projectKey));
	            }
	            if (projects.size() > 0) {
	            	if (org != null) {
	            		org.setProjects(projects);
	            		num_bind += projects.size();
	            		entitymanager.merge(org);
	            	}
	            }
	        }
            entitymanager.getTransaction().commit();
        }
        
        //////////////////entitymanager.getTransaction().commit();
        System.out.println("Imported " + num_bind + " organization-project links");
	}
	
	public void importData(String entity, boolean incrementalMode) {
				
		// initialize entity manager factory
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory("alpsv8");
		EntityManager entitymanager = emfactory.createEntityManager();
						        
        // get or create arianna source -------------------------------------------
        entitymanager.getTransaction().begin();
        Query query = entitymanager.createQuery("Select s FROM Source s where s.label = :label");
        query.setParameter("label", "Arianna - Anagrafe Nazionale delle Ricerche");
        List<Source> ariannaSource = query.getResultList();
        Source source = null;
        if (ariannaSource.size() > 0) {
        	source = ariannaSource.get(0);
        	System.out.println("Retrieved " + source.getLabel() + " source");
        } else {
        	source = setSource();
        	entitymanager.persist(source);
        	System.out.println("Created " + source.getLabel() + " source");
        }        
        List<Source> sources = new ArrayList<Source>();
    	sources.add(source);
    	entitymanager.getTransaction().commit();
    	// ------------------------------------------------------------------------ 
    	
    	if (entity.equals("all")) {    		
    		Map<String,Organization> orgMap = importOrganizations(entitymanager, sources, incrementalMode);
            Map<String,Project> projectMap = importProjects(entitymanager, sources, incrementalMode);
            importOrganizationProjectLinks(entitymanager, orgMap, projectMap);			
		}
    	
    	if (entity.equals("organization")) {			
    		Map<String,Organization> orgMap = importOrganizations(entitymanager, sources, incrementalMode);
		}
    	
    	if (entity.equals("project")) {			
    		Map<String,Project> projectMap = importProjects(entitymanager, sources, incrementalMode);
		}
    	
    	if (entity.equals("link")) {
    		Map<String,Organization> orgMap = retrieveOrganizations(entitymanager, source); 
    		Map<String,Project> projectMap = retrieveProjects(entitymanager, source);
    		if (orgMap.size() > 0 && projectMap.size() > 0) {
    			importOrganizationProjectLinks(entitymanager, orgMap, projectMap);
    		}
		}
             
		entitymanager.close();
		emfactory.close();               
        
	}

}
