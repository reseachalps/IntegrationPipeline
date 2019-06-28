package it.unimore.alps.sources.questio;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
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
import org.json.simple.JSONObject;

import com.opencsv.CSVReader;

import it.unimore.alps.sources.cercauniversita.CercaUniversitaImporter;
import it.unimore.alps.sql.model.*;

public class QuestioImporter {
	
	private String csvFile;																		// organization file 
	private CSVReader reader;																	// organization file reader
	List<String> schema = new ArrayList<String>();												// organization file schema
	List<Map<String, String>> data = new ArrayList<>(); 										// organization data
	private String sourceName = "Questio";														// data source name
	private String sourceUrl = "http://www.openinnovation.regione.lombardia.it/it/questio";		// data source url
	private String sourceRevisionDate = "10-01-2018";											// data source date
	
	public static void main(String[] args) {
		
		// BEGIN: INPUT PARAMETERS ------------------------------------------------------
		CommandLine commandLine;
		// organization file
        Option dataOption = Option.builder("data")
        	.hasArg()
            .required(true)
            .desc("The file that contains Questio data. ")
            .longOpt("data")
            .build();
        // database where to import data
        Option DB = Option.builder("DB").hasArg().required(true).desc("DB. ").longOpt("DB").build();
        
        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addOption(dataOption);
        options.addOption(DB);
        
        String questioFile = null;
        boolean header = true;
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
	        	questioFile = commandLine.getOptionValue("data");
	        	System.out.println("\tQuestio file: " + questioFile);
			} else {
				System.out.println("\tQuestio file not provided. Use the data option.");
	        	System.exit(1);
			}		
	        
	        System.out.println("----------------------------\n");
        
        } catch (org.apache.commons.cli.ParseException e) {
			e.printStackTrace();
		}
		// END: INPUT PARAMETERS --------------------------------------------------------
        
        // import data
		QuestioImporter questioImporter = new QuestioImporter(questioFile, header);
		System.out.println("Starting importing Questio data...");
		questioImporter.importData(db);

	}
	
	public QuestioImporter(String inputFile, boolean header) {
		
		// read and load in memory the input CSV file
		csvFile = inputFile;
		try {
			reader = new CSVReader(new FileReader(csvFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		// extract the header from csv file
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
	
	private Organization setOrganizationFields(Map<String,String> data, List<OrganizationExtraField> orgExtraFields, List<Source> sources) {
		
		// save organization information into Organization object		
		Organization org = null;
		
		if (!data.get("Denominazione").equals("")) {
			if (org == null)
				org = new Organization();
			org.setLabel(data.get("Denominazione"));			
		}
		
	    if (!data.get("Anno costituzione").equals("")) {
	    	SimpleDateFormat formatter = new SimpleDateFormat("yyyy");
	    	Date utilDate=null;
			try {
				utilDate = formatter.parse(data.get("Anno costituzione"));
			} catch (ParseException e) {
				e.printStackTrace();
			}
	    	if (utilDate != null) {
	    		if (org == null) {
					org = new Organization();
	    		}
	    		org.setCreationYear(utilDate);
	    	}
	    }
	    
	    String address = data.get("Sede");
	    if (!address.equals("")) {
	    	if (org == null) {
				org = new Organization();				
	    	}
	    	org.setAddressSources(sources);
	    	List<String> addressFields = Arrays.asList(address.split("\\|"));
	    	org.setAddress(addressFields.get(0));
	    	
	    	List<String> locationFields = new ArrayList<>();
	    	locationFields.addAll(Arrays.asList(addressFields.get(1).split(" ")));
	    	org.setPostcode(locationFields.get(0));	    	
	    	
	    	// country and countrycode
	    	if (locationFields.get(locationFields.size()-1).equals("Italia")) {
    			org.setCountry("Italy");	    		
    			org.setCountryCode("IT");
    			
    			if (locationFields.size() == 3) {
    	    		// city
    				String cleanCity = locationFields.get(1).substring(0, 1).toUpperCase() + locationFields.get(1).substring(1);
    	    		org.setCity(cleanCity);
    	    	} else {
    	    		if (locationFields.size() > 3) {
    	    			// urban unit
    	    			//org.setUrbanUnit(locationFields.get(locationFields.size()-2));
    	    			int [] indexes = {locationFields.size()-1,locationFields.size()-2,0};
    	    			for (int i : indexes) {
    	    				locationFields.remove(i);
    	    			}
    	    			// city
    	    			String city = String.join(" ", locationFields);
    	    			String cleanCity = city.substring(0, 1).toUpperCase() + city.substring(1);
    	    			org.setCity(cleanCity);
    	    		}
    	    	}
    		} else {
    			return null;
    		}
	    	
	    	for(int i=2; i<addressFields.size(); i++) {
	    		List<String> extraFields = Arrays.asList(addressFields.get(i).split(" "));
	    		String key = extraFields.get(0).substring(0, extraFields.get(0).length() - 1);
	    		StringBuilder value = new StringBuilder();
	    		for (int j=1; j<extraFields.size(); j++) {
	    			value.append(extraFields.get(j));
	    		}
		    	String fieldValue = value.toString();
		    	OrganizationExtraField orgExtraField = new OrganizationExtraField();
		    	orgExtraField.setFieldKey(key);
		    	orgExtraField.setFieldValue(fieldValue);
		    	orgExtraField.setOrganization(org);
		    	orgExtraFields.add(orgExtraField);
	    	}
	    	
	    }
	    
	    if (!data.get("Totale individui").equals("")) {
	    	if (org == null)
				org = new Organization();
	    	org.setFinancePrivateEmployees(data.get("Totale individui"));
	    	SimpleDateFormat formatter = new SimpleDateFormat("yyyy");
	    	Date utilDate=null;
			try {
				utilDate = formatter.parse("2013");
			} catch (ParseException e) {
				e.printStackTrace();
			}
	    	if (utilDate != null) {
	    	 	org.setFinancePrivateDate(utilDate);
	    	}
	    }
	    
	    if (!data.get("Ricavi totali").equals("")) {
	    	if (org == null)
				org = new Organization();
	    	String revenue = data.get("Ricavi totali");
	    	Float rev = Float.parseFloat(revenue.substring(2).replace(".", "").split(",")[0]);
	    	String final_revenue = ""+rev;
	    	org.setFinancePrivateRevenueRange(final_revenue);
	    	if (org.getFinancePrivateDate() == null) {
		    	SimpleDateFormat formatter = new SimpleDateFormat("yyyy");
		    	Date utilDate=null;
				try {
					utilDate = formatter.parse("2013");
				} catch (ParseException e) {
					e.printStackTrace();
				}
		    	if (utilDate != null) {
		    	 	org.setFinancePrivateDate(utilDate);
		    	}
	    	}
	    }
	    	    
	    if (!data.get("Pmi").equals("")) {
	    	if (org == null)
				org = new Organization();
	    	if (data.get("Pmi").equals("Si")) {
	    		org.setTypeCategoryCode("PMI");
	    		org.setTypeLabel("Piccola e media impresa");
	    		org.setTypeKind("Privata");
	    		org.setIsPublic("false");
	    	}
	    }		  
	    
	    return org;
		
	}
	
	private String tryToMatchCode(String code, Map<String, OrganizationActivity> mapOrgAct) {
		String testCode = code;
		boolean found = false;
		
		while(testCode.length()>0) {
			if (mapOrgAct.get(testCode)!=null) {
				found = true;
				break;
			} else {
				testCode = testCode.substring(0,testCode.length() - 1);
			}
		}
		
		if (found) {
			return testCode;			
		}
		return code;
		
	}
	
	private List<OrganizationActivity> setOrganizationActivities(EntityManager em, Map<String,String> data, Map<String, OrganizationActivity> mapOrgAct) {
		
		// save organization activity information into OrganizationActivity object
		List<OrganizationActivity> orgActivities = new ArrayList<OrganizationActivity>();		
		
		String activityCode = data.get("Codici ateco"); 
		if (!activityCode.equals("")) {
			
	    	List<String> codesList = Arrays.asList(activityCode.split(" "));
	    	Set<String> codes =new HashSet<>();
	    	codes.addAll(codesList);

	    	for (String code: codes) {
	    		
	    		if (code!=null && code.length() > 2 && !code.contains(".")) {
	    			code = code.replaceAll("(.{2})", "$1.");
	    			if(code.endsWith(".")) {
	    				code = code.substring(0,code.length() - 1);
	    			}
	    		}
	    		
	    		code = tryToMatchCode(code, mapOrgAct);
	    		
	    		OrganizationActivity orgActivity = null;
	    		if (mapOrgAct.get(code)!=null) {
	    			orgActivity = mapOrgAct.get(code);
	    		} else {
	    			orgActivity = new OrganizationActivity();
		    		orgActivity.setCode(code);
		    		orgActivity.setType("Ateco");		    		
		    		em.persist(orgActivity);
		    		mapOrgAct.put(code, orgActivity);
	    		}
	    		
	    		boolean insert = true;
	    		for(OrganizationActivity a: orgActivities) {
	    			if (a!=null) {
	    				if (a.getCode().equals(code)) {
	    					insert = false;
	    					break;
	    				}
	    			}
	    		}
	    		
	    		if (insert) {
	    			orgActivities.add(orgActivity);
	    		}	    		
		    	
	    	}
	    }
		
		return orgActivities;
	}
	
	private Link setOrganizationLinks(Map<String,String> data) {
		
		// save organization website information into Link object
		Link link = null;
		
		if (!data.get("Sito web").equals("")) {
	    	if (link == null)
	    		link = new Link();
	    	link.setUrl(data.get("Sito web"));
	    	link.setType("main");
	    	link.setLabel("homepage");
	    }
	    
	    return link;
	}
	

	private List<OrganizationRelation> setOrganizationRelations(Map<String,String> data, Organization org) {
		
		// save organization relationships into OrganizationRelation objects
		List<OrganizationRelation> orgRelations = new ArrayList<OrganizationRelation>();
		
		if (!data.get("Collaborazioni aziende aziende").equals("")) {
			List<String> relatedOrgNames = Arrays.asList(data.get("Collaborazioni aziende aziende").split("\\|"));
			for (String relatedOrgName: relatedOrgNames) {
				OrganizationRelation orgRel = new OrganizationRelation();
				orgRel.setLabel(relatedOrgName);
				orgRel.setOrganization(org);
				orgRelations.add(orgRel);
			}
	    }
		
		if (!data.get("Collaborazioni aziende crtt").equals("")) {
			List<String> relatedOrgNames = Arrays.asList(data.get("Collaborazioni aziende crtt").split("\\|"));
			for (String relatedOrgName: relatedOrgNames) {
				OrganizationRelation orgRel = new OrganizationRelation();
				orgRel.setLabel(relatedOrgName);
				orgRel.setOrganization(org);
				orgRelations.add(orgRel);
			}
	    }
		
    	
    	return orgRelations;
	}
	
	private List<OrganizationExtraField> setExtraOrganizationFields(Map<String,String> data, Set<String> attributes, Organization org) {
		
		// save organization extra information into OrganizationExtraField objects
		List<OrganizationExtraField> orgExtraFields = new ArrayList<OrganizationExtraField>();
				
		for (String key : data.keySet()) {
			if (!attributes.contains(key)) {
				if (!data.get(key).equals("")) {
					OrganizationExtraField orgExtraField = new OrganizationExtraField();
					orgExtraField.setFieldKey(key);
					orgExtraField.setFieldValue(data.get(key));
					orgExtraField.setOrganization(org);
					orgExtraField.setVisibility(true);
					orgExtraFields.add(orgExtraField);
				}
			}
		}
		
		return orgExtraFields;
	}
	
	private Source setSource() {
		
		// save data source information
		Source source = new Source();
		source.setLabel(sourceName);
		source.setUrl(sourceUrl);
		DateFormat df = new SimpleDateFormat("dd-MM-yyyy");
        
		Date sourceDate;
		try {
			sourceDate = df.parse(sourceRevisionDate);
			source.setRevisionDate(sourceDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return source;		
	}
	
	private List<Project> setOrganizationProjects(Map<String,String> data) {
		List<Project> projects = new ArrayList<Project>();
		
    	if (!data.get("Denominazione").equals("Centro Ricerche Produzioni Animali - CRPA S.p.A.") && !data.get("Denominazione").equals("Fondazione Politecnico di Milano") && !data.get("Denominazione").equals("Telematic Solutions Advanced Technology SpA")) {
        	if (!data.get("Progetti").equals("")) {
        		List<String> projectList = Arrays.asList(data.get("Progetti").split("\\|"));
        		for (int j=0; j<projectList.size(); j+=2) {
        			String projectDescription = projectList.get(j);
        			String projectYear = projectList.get(j+1);
        			
        			Pattern p = Pattern.compile("(\\d{4})");
        			Matcher m = p.matcher( projectYear );

        			Project project = new Project();
        			project.setDescription(projectDescription);        			
        			
        			Set<String> years = new HashSet<String>();
        			while (m.find()) {
        				years.add(m.group());
        	        }
        			List<Integer> yearsList = new ArrayList<>();
        			for (String singleYear: years) {
        				yearsList.add(Integer.parseInt(singleYear));
        			}
        			
        			if (yearsList.size() == 1) {
        				project.setYear(""+yearsList.get(0));
        			} else {
        				if (yearsList.size() > 1) {
        					int recentYear = Collections.max(yearsList);
        					int oldYear = Collections.min(yearsList);
        					if (recentYear > 2050) {
        						project.setYear(""+oldYear);
        					} else {
        						int duration = (recentYear - oldYear)*12;
            					project.setYear(""+recentYear);
            					if (duration < 200) {
            						project.setDuration(""+duration);
            					}
        					}        					        					
        				}
        			}        					
        			
        			projects.add(project);
        		}
        	}
    	}
    	
    	return projects;
	}
	
	public static String getLinks(List<Link> links) {

		// retrieve organization websites
		String linkId = null;
		
		if (links != null) {
			List<String> linksNames = new ArrayList<>();
			for (Link link: links) {
				linksNames.add(link.getUrl());
			}
			linkId = String.join("_", linksNames);
		}
		
		return linkId;
	}
	
	private OrganizationType setOrganizationType(EntityManager entitymanager, Map<String,String> data, Map<String, OrganizationType> mapOrgType) {
		
		// save organization type information into OrganizationType object
		OrganizationType orgType = null;
		
		String orgTypology = data.get("Tipologia");
		if (!orgTypology.equals("")) {
			orgTypology = getCleanedString(orgTypology);
			if (mapOrgType.get(orgTypology) != null) {
				orgType = mapOrgType.get(orgTypology);
			} else {
				orgType = new OrganizationType();
				orgType.setLabel(orgTypology);
				entitymanager.persist(orgType);
				mapOrgType.put(orgTypology, orgType);
				entitymanager.persist(orgType);
			}
		}
		
		return orgType;

	}
	
	private String getCleanedString(String s) {
		// capitalize first letter of the input string
		return s.substring(0,1).toUpperCase() + s.substring(1).toLowerCase();
	}
	
	public void importData(String db) {
			
		// initialize entity manager factory
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();		
		
		Set<String> attributes = new HashSet<String>(Arrays.asList("Progetti", "Denominazione", "Anno costituzione", "Sede", "Totale individui", "Ricavi totali", "Pmi", "Tipologia", "Codici ateco", "Sito web", "Collaborazioni aziende aziende", "Collaborazioni aziende crtt"));
		String[] line;
		// read data and store it in a map
        try {
            while ((line = reader.readNext()) != null) {
            	Map<String, String> row_data = new HashMap<String, String>();
	        	for(int i=0; i< line.length; i++) {
	        		String key = schema.get(i);
	        		String value = line[i];
	        		row_data.put(key, value);
	        	}
	        	data.add(row_data);
	        	
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        Source source = setSource();
        entitymanager.persist(source);
        List<Source> sources = new ArrayList<Source>();
    	sources.add(source);

        // read data and import it into the database
    	entitymanager.getTransaction().begin();
    	Map<String, OrganizationType> mapOrgType = new HashMap<>();
    	Query queryType = entitymanager.createQuery("Select t FROM OrganizationType t");        
        List<OrganizationType> types = queryType.getResultList();
        if (types != null && types.size() > 0) {
        	for(OrganizationType t: types) {
        		mapOrgType.put(t.getLabel(), t);
        	}
        } 
    	
    	Map<String, OrganizationActivity> mapOrgAct = new HashMap<>();
    	Query queryActivity = entitymanager.createQuery("Select a FROM OrganizationActivity a");        
        List<OrganizationActivity> acts = queryActivity.getResultList();
        if (acts != null && acts.size() > 0) {
        	for(OrganizationActivity act: acts) {
        		mapOrgAct.put(act.getCode(), act);
        	}
        }     	    	    	    	
    	
    	int numOrgs = 0;
    	int numProjects = 0;
        for(int i=0; i<data.size(); i++) {
        	
        	if ((i % 100) == 0) {
        		System.out.println(i);
        	}        	
        	
        	Map<String, String> row_data = data.get(i);
        		
        	// set main organization fields
        	List<OrganizationExtraField> orgExtraFields1 = new ArrayList<OrganizationExtraField>();
        	Organization org = setOrganizationFields(row_data, orgExtraFields1, sources);
        	if (org == null) {
        		continue;
        	}
        	
        	// organization activities
        	List<OrganizationActivity> activities = setOrganizationActivities(entitymanager, row_data, mapOrgAct);
        	// connect the activities to the related organization
        	if (org != null) {
        		numOrgs++;
        		org.setActivities(activities);
        	}
        	
        	// organization links
        	Link link = setOrganizationLinks(row_data);
        	if (link != null) {
        		link.setSources(sources);
        		entitymanager.persist(link);
        		if (org != null) {
	        		List<Link> orgLinks = new ArrayList<Link>();
	        		orgLinks.add(link);
	        		// connect the link to the related organization
	        		org.setLinks(orgLinks);	        		       			        		
        		}        		        	
        	}
        	
        	// organization relations
        	if (org != null) {
        		List<OrganizationRelation> orgRels = setOrganizationRelations(row_data, org);
        		for(OrganizationRelation orgRel: orgRels) {
        			entitymanager.persist(orgRel);
        		}
        	}
        	
        	if (org != null) {
        		OrganizationType orgType = setOrganizationType(entitymanager, row_data, mapOrgType);
				if (orgType != null) {
					org.setOrganizationType(orgType);
				}
        	}
        	
        	// connect the source to the related organization
        	if (org != null) {
        		org.setSources(sources);        		        	
        	}
        	
        	// organization extra fields
        	if (org != null) {
	        	List<OrganizationExtraField> orgExtraFields = setExtraOrganizationFields(row_data, attributes, org);
	        	for(OrganizationExtraField orgExtraField1: orgExtraFields1) {
	        		orgExtraFields.add(orgExtraField1);
	        	}
	        	for (OrganizationExtraField orgExtraField: orgExtraFields) {
	        		entitymanager.persist(orgExtraField);
	        	}
        	}
        	
        	// organization projects
        	List<Project> orgProjects = setOrganizationProjects(row_data);
        	numProjects += orgProjects.size();
        	for (Project orgProject: orgProjects) {
        		// connect the source to the related project
        		orgProject.setSources(sources);
        		entitymanager.persist(orgProject);
        	}
        	// connect the projects to the related organization
        	if (orgProjects.size() > 0) {
        		if (org != null) {
        			org.setProjects(orgProjects);
        		}
        	}
        	
        	if (org != null) {
        		// set unique identifier
        		List<Link> orgLinks;
        		if (link!=null) {
        			orgLinks = new ArrayList<Link>();
        			orgLinks.add(link);
        		} else {
        			orgLinks = org.getLinks();
        		}
        		String identifier = sourceName + "::" + org.getLabel() + "::" + getLinks(orgLinks) + "::" + org.getCity() + "::" + orgProjects.size();
				OrganizationIdentifier orgId = new OrganizationIdentifier();
				orgId.setIdentifier(identifier);
				orgId.setProvenance(sourceName);
				orgId.setIdentifierName("lid");
				orgId.setVisibility(false);
				orgId.setOrganization(org);
				entitymanager.persist(orgId);
        	}
        	
        	entitymanager.persist(org);        	
        }
        
        entitymanager.getTransaction().commit();
        System.out.println("Imported " + numOrgs + " organizations and " + numProjects + " projects");
        
		entitymanager.close();
		emfactory.close();
         
	}

}
