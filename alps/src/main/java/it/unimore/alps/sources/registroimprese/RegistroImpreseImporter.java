package it.unimore.alps.sources.registroimprese;

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

import it.unimore.alps.sql.model.Link;
import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationActivity;
import it.unimore.alps.sql.model.OrganizationExtraField;
import it.unimore.alps.sql.model.OrganizationIdentifier;
import it.unimore.alps.sql.model.OrganizationType;
import it.unimore.alps.sql.model.Project;
import it.unimore.alps.sql.model.ProjectExtraField;
import it.unimore.alps.sql.model.ProjectIdentifier;
import it.unimore.alps.sql.model.Source;

public class RegistroImpreseImporter {
	
	private CSVReader smeReader;												// small-medium enterprise file reader
	private CSVReader startupReader;											// startup file reader
	private CSVReader incubatorReader;											// incubator file reader
	List<String> smeSchema = new ArrayList<String>();							// small-medium enterprise file schema
	List<String> startupSchema = new ArrayList<String>();						// startup file schema
	List<String> incubatorSchema = new ArrayList<String>();						// incubator file schema
	List<Map<String, String>> smeData = new ArrayList<>();						// small-medium enterprise data
	List<Map<String, String>> startupData = new ArrayList<>();					// startup data
	List<Map<String, String>> incubatorData = new ArrayList<>();				// incubator data
	private String sourceName = "Startup - Registro delle imprese";				// data source name
	private String sourceUrl = "http://startup.registroimprese.it/isin/home";	// data source url
	private String sourceRevisionDate = "01-06-2019";							// data source date
	
	
	public static void main(String[] args) {
		
		// BEGIN: INPUT PARAMETERS ------------------------------------------------------
		CommandLine commandLine;
		// small-medium enterprise file
        Option pmiFileOption = Option.builder("smeFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains sme data. ")
	            .longOpt("smeFile")
	            .build();
        // startup file
        Option startupFileOption = Option.builder("startupFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains startup data.")
	            .longOpt("startupFile")
	            .build();
        // incubator file
        Option incubatorFileOption = Option.builder("incubatorFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains enterprise incubator data.")
	            .longOpt("incubatorFile")
	            .build();
        // database where to import data
        Option DB = Option.builder("DB").hasArg().required(true).desc("DB. ").longOpt("DB").build();
        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addOption(pmiFileOption);
        options.addOption(startupFileOption);
        options.addOption(incubatorFileOption);
        options.addOption(DB);
        
        String smeFile = null;
        String startupFile = null;
        String incubatorFile = null;
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

	        if (commandLine.hasOption("smeFile")) {   
	        	smeFile = commandLine.getOptionValue("smeFile");
	        	System.out.println("\tSme file: " + smeFile);
			} else {
				System.out.println("\tSme file not provided. Use the smeFile option.");
	        	System.exit(1);
			}
			
			if (commandLine.hasOption("startupFile")) {
				startupFile = commandLine.getOptionValue("startupFile");
				System.out.println("\tStartup file: " + startupFile);
			} else {
				System.out.println("\tStartup file not provided. Use the startupFile option.");
	        	System.exit(1);
			}
	
			if (commandLine.hasOption("incubatorFile")) {
				incubatorFile = commandLine.getOptionValue("incubatorFile");
				System.out.println("\tIncubator file: " + incubatorFile);
			} else {
				System.out.println("\tIncubator file not provided. Use the incubatorFile option.");
				System.exit(1);
			}  								
			
			System.out.println("----------------------------\n");
        
        } catch (org.apache.commons.cli.ParseException e) {
			e.printStackTrace();
		}
		// END: INPUT PARAMETERS --------------------------------------------------------
        
        // import data
        RegistroImpreseImporter riImporter = new RegistroImpreseImporter(smeFile, startupFile, incubatorFile, header);
        riImporter.importData(db);

	}
	
	public RegistroImpreseImporter(String smeFile, String startupFile, String incubatorFile, boolean header) {
		
		// read and load in memory the input CSV file
		this.smeReader = initializeCSVReader(smeFile, header, smeSchema);
		this.startupReader = initializeCSVReader(startupFile, header, startupSchema);
		this.incubatorReader = initializeCSVReader(incubatorFile, header, incubatorSchema);
		
		this.smeData = createMapFromFileCSV(smeReader, smeSchema);
		this.startupData = createMapFromFileCSV(startupReader, startupSchema);
		this.incubatorData = createMapFromFileCSV(incubatorReader, incubatorSchema);
	}
	
	private CSVReader initializeCSVReader(String inputFile, boolean header, List<String> schema) {
		
		CSVReader reader = null;
		
		// load the input CSV file in memory
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
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return source;		
	}
	
	private List<Map<String, String>> createMapFromFileCSV(CSVReader reader, List<String> schema) {
		
		List<Map<String, String>> fullData = new ArrayList<Map<String, String>>();
		
		// read data and store it in a map
		String[] line;
        try {
            while ((line = reader.readNext()) != null) {
            	Map<String, String> rowData = new HashMap<String, String>();
	        	for(int i=0; i< line.length; i++) {
	        		String key = schema.get(i);
	        		String value = line[i];
	        		rowData.put(key, value.toString());
	        	}
	        	fullData.add(rowData);
	        	
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return fullData;
        
	}
	
	private Organization setOrganizationFields(Map<String,String> data, List<Source> sources, String orgType) {
		
		// save organization information into Organization object
		Organization org = new Organization();
		
    	String label = data.get("denominazione");
		if (!label.equals("")) {
			label = label.trim().replaceAll(" +", " ");
			org.setLabel(label);
		}
	      
	    String city = data.get("comune");
	    if (!city.equals("")) {
	    	org.setAddressSources(sources);
	    	String cleanCity = getCleanedString(city);
	    	org.setCity(cleanCity);
	    }
	    
	    /*String urbanUnit = data.get("pv");
	    if (!urbanUnit.equals("")) {
	    	org.setUrbanUnit(urbanUnit);
	    }*/
	    
	    String creationDateString = data.get("data inizio dell'esercizio effettivo dell'attività");
	    if (!creationDateString.equals("")) {
	    	
	    	DateFormat df = new SimpleDateFormat("dd/MM/yyyy");
	        
			try {
				Date startDate = df.parse(creationDateString);
				org.setCreationYear(startDate);
			} catch (ParseException e) {
				e.printStackTrace();
			}
	    }	  
	    
	    org.setCountry("Italy");
	    org.setCountryCode("IT");
	    
	    org.setTypeLabel(orgType);
	    org.setTypeKind("Privata");
	    org.setIsPublic("false");
	    
	    return org;
		
	}
	
	private Link setOrganizationLinks(Map<String,String> data) {
		
		// save organization website information into Link object
		Link link = null;
		
		String url = data.get("sito internet");
		if (!url.equals("")) {
	    	link = new Link();
	    	if (!url.contains("http://") && !url.contains("https://")) {
	    		url = "http://" + url;
	    	}
	    	link.setUrl(url);
			link.setType("main");
			link.setLabel("homepage");
	    }
	    
	    return link;
	}
	
	private String getCleanedString(String s) {
		// capitalize first letter of input string
		return s.substring(0,1).toUpperCase() + s.substring(1).toLowerCase();
	}
	
	private OrganizationType setOrganizationType(Map<String,String> data, EntityManager entitymanager, Map<String, OrganizationType> orgTypeMap) {
		
		// save organization type information into OrganizationType object
		OrganizationType orgType = null;
		
		String type = data.get("nat.giuridica");
		type = getCleanedString(type);
		if(!type.equals("")) {
			
			orgType = orgTypeMap.get(type);
			if (orgType == null) {
				orgType = new OrganizationType();
	        	orgType.setLabel(type);
	        	entitymanager.persist(orgType);
	        	orgTypeMap.put(type, orgType);
			}
		}
		
		return orgType;
	}
	
	private List<OrganizationIdentifier> setOrganizationIdentifiers(Map<String,String> data, Organization org) {
	  	 
		// save organization identifier information into OrganizationIdentifier object
	  	List<OrganizationIdentifier> orgIds = new ArrayList<OrganizationIdentifier>();
	  	
	  	OrganizationIdentifier orgId = null;
	  	String orgIdentifier = data.get("codice fiscale");
	  	if (!orgIdentifier.equals("")) {
	  		orgId = new OrganizationIdentifier();
	  		orgId.setIdentifier(orgIdentifier);
	  		orgId.setProvenance(sourceName); 		
	  		orgId.setIdentifierName("codice fiscale");
	  		orgId.setOrganization(org);
	  		orgIds.add(orgId);
	  	}
	  	
	  	return orgIds;
	  	 
	}
		
	private List<OrganizationActivity> setOrganizationActivities(EntityManager em, Map<String,String> data, Map<String, OrganizationActivity> mapOrgAct) {
		
		// save organization activity information into OrganizationActivity objects
		List<OrganizationActivity> orgActivities = new ArrayList<OrganizationActivity>();		
		
		OrganizationActivity orgActivity = null;
		String code = data.get("ateco 2007");
		
		boolean isNumber = true;
		try {
		      int codeToInt = Integer.parseInt(code);
		} catch (NumberFormatException e) {
			isNumber = false;
		}
		
		if (code!=null && code.length() > 2) {
			code = code.replaceAll("(.{2})", "$1.");
			if(code.endsWith(".")) {
				code = code.substring(0,code.length() - 1);
			}
		}			
		if (!code.equals("") && isNumber) {
			if (mapOrgAct.get(code) != null) {
				orgActivity = mapOrgAct.get(code);
			} else {
				orgActivity = new OrganizationActivity();				
	    		orgActivity.setCode(code);
	    		orgActivity.setType("Ateco");
	    		String activity = data.get("attività"); 
	    		if (!activity.equals("")) {
	    			activity = getCleanedString(activity);
	    			orgActivity.setLabel(activity);
	    		}
	    		em.persist(orgActivity);
	    		mapOrgAct.put(code, orgActivity);
			}
			orgActivities.add(orgActivity);
			
		}
								
		return orgActivities;
	}
	
	private boolean isValidOrganization(Map<String,String> data) {
		
		// filter organization by Ateco code
		boolean isValid = false;
		
		String activity = data.get("attività"); 
		if (!activity.equals("")) {
			if (activity.contains("M 72")) {
				isValid = true;
			}
		}
		
		return isValid;
	}
	
	private List<OrganizationExtraField> setOrganizationExtraFields(Map<String,String> data, Set<String> attributes, Organization org) {
		
		// save organization extra information into OrganizationExtraField objects
		List<OrganizationExtraField> orgExtraFields = new ArrayList<OrganizationExtraField>();				
		
		for (String key : data.keySet()) {
			if (!attributes.contains(key)) {
				if (!data.get(key).equals("")) {
					OrganizationExtraField orgExtraField = new OrganizationExtraField();
					orgExtraField.setVisibility(true);
					orgExtraField.setFieldKey(key);
					orgExtraField.setFieldValue(data.get(key));
					orgExtraField.setOrganization(org);
					orgExtraFields.add(orgExtraField);
				}
			}
		}
		
		return orgExtraFields;
	}
	
	public static String getOrgIds(List<OrganizationIdentifier> orgIds) {
		
		// retrireve organization identifiers
		String orgId = null;
		
		if (orgIds != null) {
			List<String> ids = new ArrayList<>();
			for (OrganizationIdentifier oi: orgIds) {
				ids.add(oi.getIdentifierName() + "_" + oi.getIdentifier());
			}
			orgId = String.join(",", ids);
		}
		
		return orgId;
	}
	
	private void importOrg(List<Map<String, String>> orgData, EntityManager entitymanager, List<Source> sources, Map<String, OrganizationType> orgTypeMap, String type, Map<String, OrganizationActivity> mapOrgAct) {
		
		// read organization data and import it into the database 						        
		Set<String> attributes = new HashSet<String>(Arrays.asList("denominazione", "nat.giuridica", "codice fiscale", "pv", "comune", "data inizio dell'esercizio effettivo dell'attività", "ateco 2007", "sito internet"));
		
		entitymanager.getTransaction().begin();
		
        for(int i=0; i<orgData.size(); i++) {
        	if ((i % 100) == 0) {
        		System.out.println(i);
        	}
        	
        	Map<String, String> rowData = orgData.get(i);
        	
        	//if (isValidOrganization(rowData)) {	//	filter organizations by Ateco code               		        
        		
        	// set main organization fields
        	Organization org = setOrganizationFields(rowData, sources, type);
        	
        	// organization links
        	Link link = setOrganizationLinks(rowData);
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
        	
        	// set organization types
        	OrganizationType orgType = setOrganizationType(rowData, entitymanager, orgTypeMap);
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
        		        		
        		// set unique identifier
        		String identifier = sourceName + "::" + getOrgIds(orgIdentifiers);
				OrganizationIdentifier orgId = new OrganizationIdentifier();
				orgId.setIdentifier(identifier);
				orgId.setProvenance(sourceName);
				orgId.setIdentifierName("lid");
				orgId.setVisibility(false);
				orgId.setOrganization(org);
				entitymanager.persist(orgId);
        	}
        	
        	// organization activities
        	List<OrganizationActivity> activities = setOrganizationActivities(entitymanager, rowData, mapOrgAct);
        	if (activities != null) {
	        	// connect the activities to the related organization
	        	org.setActivities(activities);
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
        	
        	//}
        }               
        
        entitymanager.getTransaction().commit();
	}	
	
	public void importData(String db) {
						
		// initialize entity manager factory
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();
						        
        entitymanager.getTransaction().begin();
		
        // get or create registro imprese source -------------------------------------------
        Query query = entitymanager.createQuery("Select s FROM Source s where s.label = :label");
        query.setParameter("label", sourceName);
        List<Source> riSource = query.getResultList();
        Source source = null;
        if (riSource.size() > 0) {
        	source = riSource.get(0);
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
    	
    	System.out.println("Starting importing organizations...");
    	Map<String, OrganizationType> orgTypeMap = new HashMap<>();
    	Query queryType = entitymanager.createQuery("Select t FROM OrganizationType t");        
        List<OrganizationType> types = queryType.getResultList();
        if (types != null && types.size() > 0) {
        	for(OrganizationType t: types) {
        		orgTypeMap.put(t.getLabel(), t);
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
    	
    	importOrg(this.smeData, entitymanager, sources, orgTypeMap, "Piccola e media impresa", mapOrgAct);
    	importOrg(this.startupData, entitymanager, sources, orgTypeMap, "Startup", mapOrgAct);
    	importOrg(this.incubatorData, entitymanager, sources, orgTypeMap, "Incubatore", mapOrgAct);    	    	
             
		entitymanager.close();
		emfactory.close();               
        
	}

}

