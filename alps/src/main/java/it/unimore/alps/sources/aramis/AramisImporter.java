package it.unimore.alps.sources.aramis;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.DateFormat;
import java.text.Normalizer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
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
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.opencsv.CSVReader;

import it.unimore.alps.sql.model.Leader;
import it.unimore.alps.sql.model.Link;
import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationActivity;
import it.unimore.alps.sql.model.OrganizationExtraField;
import it.unimore.alps.sql.model.OrganizationIdentifier;
import it.unimore.alps.sql.model.OrganizationType;
import it.unimore.alps.sql.model.Person;
import it.unimore.alps.sql.model.PersonIdentifier;
import it.unimore.alps.sql.model.Project;
import it.unimore.alps.sql.model.ProjectExtraField;
import it.unimore.alps.sql.model.ProjectIdentifier;
import it.unimore.alps.sql.model.Source;
import it.unimore.alps.sql.model.Thematic;
import it.unimore.alps.idgenerator.LocalIdentifierGenerator;

public class AramisImporter {
	
	private String dataFile;													// file containing the data to be imported
	private CSVReader dataReader;												// file reader
	List<String> dataSchema = new ArrayList<String>();							// CSV file schema
	List<Map<String, String>> data = new ArrayList<>();							// CSV file content
	private String sourceName = "ARAMIS";										// data source name
	private String sourceUrl = "https://www.aramis.admin.ch/?Sprache=en-US";	// data source url
	private String sourceRevisionDate = "15-10-2018";							// data source date
	
	
	public static void main(String[] args) {
		
		// BEGIN: INPUT PARAMETERS ------------------------------------------------------
		CommandLine commandLine;
		// file containing the data to be imported
        Option dataOption = Option.builder("data")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains project and organization data. ")
	            .longOpt("data")
	            .build();    
        // database where to import data
        Option DB = Option.builder("DB").hasArg().required(true).desc("DB. ").longOpt("DB").build();

        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addOption(dataOption);
        options.addOption(DB);
        
        String dataFile = null;
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
	        	dataFile = commandLine.getOptionValue("data");
	        	System.out.println("\tData file: " + dataFile);
			} else {
				System.out.println("\tData file not provided. Use the data option.");
	        	System.exit(1);
			}	        						
			
			System.out.println("----------------------------\n");
        
        } catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        // END: INPUT PARAMETERS --------------------------------------------------------
		
        // import data
        AramisImporter aramisImporter = new AramisImporter(dataFile, header);
        aramisImporter.importData(db);

	}
	
	public AramisImporter(String dataFile, boolean header) {
		
		this.dataFile = dataFile;
		// read CSV file and load in memory
		this.dataReader = initializeCSVReader(dataFile, header, dataSchema);	
		
	}
	
	private CSVReader initializeCSVReader(String inputFile, boolean header, List<String> schema) {
	
		CSVReader reader = null;
		
		// load CSV file in memory
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return source;		
	}	
	
	private Project setProjectFields(EntityManager entitymanager, Map<String,String> data, List<Source> sources) {
		
		// save project information into Project object
		Project project= new Project();

		String title = (String) data.get("PROJEKTTITEL_E");
		if (title == null || title.equals("")) {
			title = (String) data.get("PROJEKTTITEL");
		}
		if (title != null && !title.equals("")) {
			project.setLabel(title);
		}		
		
		String descr = (String) data.get("0210_Kurzbeschreibung_DE_Text_Oeff");
		if (descr == null || descr.equals("")) {
			descr = (String) data.get("0210_Kurzbeschreibung_FR_Text_Oeff");
		}
		if (descr != null && !descr.equals("")) {
			project.setDescription(descr);
		}
		
		Date projectStartDate = null;
		DateFormat df = new SimpleDateFormat("dd/MM/yyyy");
		String startDate = (String) data.get("STARTDATUM_EFFEKTIV");
		if (startDate != null) {
			       			
			try {
				projectStartDate = df.parse(startDate);
				project.setStartDate(projectStartDate);
			} catch (ParseException e) {
				e.printStackTrace();
			}			
		}
		
		if (startDate != null) {
			Calendar startCalendar = new GregorianCalendar();
			startCalendar.setTime(projectStartDate);
			int month = startCalendar.get(Calendar.MONTH)+1;
			int year = startCalendar.get(Calendar.YEAR);
			project.setMonth(""+month);
			project.setYear(""+year);
			
			String end = (String) data.get("Enddatum");
			if (end != null && !end.equals("")) {

				Date endDate;
				try {
					endDate = df.parse(end);						
					Calendar endCalendar = new GregorianCalendar();
					endCalendar.setTime(endDate);
					int diffYear = endCalendar.get(Calendar.YEAR) - startCalendar.get(Calendar.YEAR);
					int diffMonth = diffYear * 12 + endCalendar.get(Calendar.MONTH) - startCalendar.get(Calendar.MONTH);
					project.setDuration(""+diffMonth);
					
				} catch (java.text.ParseException e) {
					e.printStackTrace();
				}
			}						
		}
		
		String budget = (String) data.get("GESAMTKOSTEN_BEWILLIGT");
		if (budget != null && !budget.equals("")) {
			project.setBudget(budget);
		}
		
		return project;
		
	}

	private List<ProjectIdentifier> setProjectIdentifiers(EntityManager entitymanager, Map<String,String> data, List<Source> sources, Project prj) {
	
		// save project identifier information into ProjectInformation objects
		List<ProjectIdentifier> prjIds = new ArrayList<>();
		
		String prjIdString = (String) data.get("PROJEKTID");	
		if (prjIdString != null) {
			
			ProjectIdentifier prjId = new ProjectIdentifier();
			prjId.setIdentifier(prjIdString);
			prjId.setIdentifierName("PROJEKTID");
			prjId.setProject(prj);
			prjId.setProvenance(sources.get(0).getLabel());
			prjId.setVisibility(true);
	    	prjIds.add(prjId);	    	
	    
		}		
		
		String prjNumber = (String) data.get("PROJEKTNR");		
		if (prjNumber != null) {
			
			ProjectIdentifier prjId = new ProjectIdentifier();
			prjId.setIdentifier(prjNumber);
			prjId.setIdentifierName("PROJEKTNR");
			prjId.setProject(prj);
			prjId.setProvenance(sources.get(0).getLabel());
			prjId.setVisibility(true);
	    	prjIds.add(prjId);	    	
	    
		}
		
		return prjIds;
		
	}
	
	private List<Thematic> setProjectThematics(EntityManager entitymanager, Map<String,String> data, Map<String, Thematic> thematicMap) {
		
		// save project thematic information into Thematic object
		List<Thematic> prjThematics = new ArrayList<>();	
		
		String thematicNABS = (String) data.get("NABSBEZ_DE");
		if (thematicNABS != null) {
			
			Thematic thematic = null;
			if (thematicMap.get(thematicNABS) != null) {
				thematic = thematicMap.get(thematicNABS);
			} else {
				thematic = new Thematic();
				thematic.setLabel(thematicNABS);
				thematic.setClassificationSystem("NABS");
				thematicMap.put(thematicNABS, thematic);
			}
			prjThematics.add(thematic);
		}
								
		return prjThematics;
		
	}
	
	private List<Map<String, String>> createMapFromFileCSV(CSVReader reader, List<String> schema) {
		
		// read data and store it in a map
		List<Map<String, String>> fullData = new ArrayList<Map<String, String>>();
		
		String[] line;
		Map<String, Boolean> mapProject = new HashMap<>();
        try {
            while ((line = reader.readNext()) != null) {            	
            	Map<String, String> rowData = new HashMap<String, String>();
            	
            	String prjId = line[0];
            	if (mapProject.get(prjId) != null) {
            		continue;
            	}
            	mapProject.put(prjId, true);
            	
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
	
	private String getCleanedString(String s) {
		
		// capitalize first character and remove empty spaces from string
		s = s.trim();
		s = s.substring(0,1).toUpperCase() + s.substring(1).toLowerCase();
		return s;
	}
	
	private Organization setOrganizationFields(Map<String, String> data, List<Source> sources) {
		
        // save organization information into Organization object       	
		Organization org = new Organization();
		
		String label1 = data.get("FIRMA1");
    	String label2 = data.get("FIRMA2"); 
		if (label1 != null && !label1.equals("") && label2 != null && !label2.equals("")) {
			String fullLabel = label1 + " " + label2;
			fullLabel = stripAccents(fullLabel);
			fullLabel = getCleanedString(fullLabel);
			org.setLabel(fullLabel);			
		}
	    
	    String address = data.get("STRASSE");
	    if (address != null && !address.equals("")) {
	    	org.setAddress(address);
	    	org.setAddressSources(sources);
	    }
	    
	    String zipCode = data.get("PLZ");
	    if (zipCode != null && !zipCode.equals("")) {
	    	org.setPostcode(zipCode);
	    }
	    
	    String city = data.get("ORT");
	    if (city != null && !city.equals("")) {
	    	org.setCity(city);
	    }
	    
	    String countryCode = data.get("LAND");
	    if (countryCode != null && !countryCode.equals("")) {
	    	
	    	List<String> countryCodes = Arrays.asList("AT", "CH", "DE", "FR", "IT", "LI", "SI");
	    	if (countryCodes.contains(countryCode)) {
	 	    	org.setCountryCode(countryCode);
		    			    			    			    	
		    	if (countryCode.equals("AT")) {
		    		org.setCountry("Austria");
		    	}
		    	if (countryCode.equals("CH")) {
		    		org.setCountry("Switzerland");
		    	}
		    	if (countryCode.equals("DE")) {
		    		org.setCountry("Germany");
		    	}
		    	if (countryCode.equals("FR")) {
		    		org.setCountry("France");
		    	}
		    	if (countryCode.equals("IT")) {
		    		org.setCountry("Italy");
		    	}
		    	if (countryCode.equals("LI")) {
		    		org.setCountry("Liechtenstein");
		    	}
		    	if (countryCode.equals("SI")) {
		    		org.setCountry("Slovenia");
		    	}
		    	
	    	} else {
	    		return null;
	    	}
	    }
	    
	    org.setSources(sources);
    	
	    return org;

	}
	
	public Map<String, Project> importProjects(EntityManager entitymanager, List<Source> sources, Map<String, Thematic> thematicMap) {
		
		System.out.println("Starting importing projects...");

		Map<String, Project> prjMap = new HashMap<>();		
		entitymanager.getTransaction().begin();
		
		// read file CSV line by line
		int numProjects = 0;
		for(int i=0; i<this.data.size(); i++) {
        	if ((i % 1000) == 0) {
        		System.out.println(i);
        	}
        	
        	Map<String, String> prjData = this.data.get(i);
        	
			String prjIdString = (String) prjData.get("PROJEKTID");	
			if (prjIdString != null) {
				if (prjMap.get(prjIdString) != null) {
					continue;
				}
			}        	        
        	
        	// set main project fields
			Project prj = setProjectFields(entitymanager, prjData, sources);
			
			// sources
			prj.setSources(sources);
			
			// project identifiers
			List<ProjectIdentifier> prjIds = setProjectIdentifiers(entitymanager, prjData, sources, prj);
			if (prjIds != null && prjIds.size() > 0) {
				prj.setProjectIdentifiers(prjIds);
			}
					
			// set project thematics
			List<Thematic> thematics = setProjectThematics(entitymanager, prjData, thematicMap);
			if (thematics != null && thematics.size() > 0) {
				prj.setThematics(thematics);
			}									
			
			if (prj != null) {
				prjMap.put(prjIdString, prj);
				numProjects+=1;
				entitymanager.persist(prj);
			}
						        	
		}
		
		entitymanager.getTransaction().commit();
		
		System.out.println("Imported " + numProjects + " projects.");
		
		return prjMap;
		
	}
	
	private void connectOrgWithProjects(EntityManager entitymanager, Map<String, String> data, Organization org, Map<String, Project> prjMap) {
			
		// connect organization with projects
		String prjIdString = (String) data.get("PROJEKTID");	
		if (prjIdString != null) {			
			
			Project prj = prjMap.get(prjIdString);
			if (prj != null) {							
			
				List<Project> orgProjects = org.getProjects();
				if (orgProjects != null && orgProjects.size() > 0) {
					orgProjects.add(prj);
					org.setProjects(orgProjects);
				} else {
					orgProjects = new ArrayList<>();
					orgProjects.add(prj);
					org.setProjects(orgProjects);
				}
				
			}
		}		
		
	}	
	
	private void connectOrgWithPeople(EntityManager entitymanager, Map<String, String> data, Organization org, Map<String, Person> personMap) {				
		
		String personId = (String) data.get("PROJEKTBETEILIGTER");	
		if (personId != null) {						
			
			Person person = personMap.get(personId);
			if (person != null) {							
			
				List<Person> orgPeople = org.getPeople();
				if (orgPeople != null && orgPeople.size() > 0) {
					boolean insert = true;
					for (Person p: orgPeople) {
						if (p.getFirstName() == null) {
							if (p.getLastName().equals(person.getLastName())) {
								insert = false;
								break;
							}
						} else {
							if ((p.getFirstName().equals(person.getFirstName()) && p.getLastName().equals(person.getLastName()))) {
								insert = false;
								break;
							}
						}
					}
					if (insert) {
						entitymanager.persist(person);
						orgPeople.add(person);
						org.setPeople(orgPeople);
					}
				} else {
					orgPeople = new ArrayList<>();
					//entitymanager.persist(person);
					orgPeople.add(person);
					org.setPeople(orgPeople);
				}
				
			}
		}		
		
	}
	
	private Person setPersonFields(EntityManager entitymanager, Map<String, String> data) {
		
		// save person information into Person object
		Person person = new Person();
		
		String lastName = data.get("NAME");
		if (lastName != null && !lastName.equals("")) {
			person.setLastName(lastName);
		}
		
		String firstName = data.get("VORNAME");
		if (firstName != null && !firstName.equals("")) {
			person.setFirstName(firstName);
		}
		
		String email = data.get("EMAIL");
		if (email != null && !email.equals("")) {
			person.setEmail(email);
		}
		
		if (person.getLastName() == null) {
			return null;
		}
		
		return person;
		
		
	}
	
	private Map<String, Person> importPeople(EntityManager entitymanager, List<Source> sources) {
		
		// import people information
		System.out.println("Starting importing people...");

		Map<String, Person> personMap = new HashMap<>();
		
		entitymanager.getTransaction().begin();
		
		int numPeople = 0;
		
		for(int i=0; i<this.data.size(); i++) {
        	if ((i % 1000) == 0) {
        		System.out.println(i);
        	}
        	
        	Map<String, String> personData = this.data.get(i);
        	
			String personId = (String) personData.get("PROJEKTBETEILIGTER");	
			if (personId != null) {
				if (personMap.get(personId) != null) {
					continue;
				}
			}        	        
        	
        	// set main person fields
			Person person = setPersonFields(entitymanager, personData);																
			
			if (person != null) {
				// sources
				person.setSources(sources);	
				
				personMap.put(personId, person);
				numPeople+=1;
				entitymanager.persist(person);
			}			
						        	
		}
		
		entitymanager.getTransaction().commit();
		
		System.out.println("Imported " + numPeople + " people.");
		
		return personMap;
		
	}
	
	public static String stripAccents(String s) {
		// normalize accents in string
	    s = Normalizer.normalize(s, Normalizer.Form.NFD);
	    s = s.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "");
	    return s;
	}
	
	private List<OrganizationIdentifier> setOrganizationIdentifiers(Map<String, String> data, Organization org) {
		
		// set unique local identifier (lid)
		String fullLabel = org.getLabel();
		String address = org.getAddress();
    	List<OrganizationIdentifier> orgIds = new ArrayList<OrganizationIdentifier>();
	  	if (fullLabel != null) {
	  		OrganizationIdentifier orgId = new OrganizationIdentifier();
	  		orgId.setIdentifier(sourceName + "::" + fullLabel);
	  		orgId.setProvenance(sourceName); 		
	  		orgId.setIdentifierName("lid");
	  		orgId.setVisibility(false);
	  		orgId.setOrganization(org);
	  		orgIds.add(orgId);
	  	}
	  	
	  	return orgIds;
	}
	
	public void importOrganizations(EntityManager entitymanager, List<Source> sources, Map<String, Project> prjMap, Map<String, Person> personMap) {
		
		System.out.println("Starting importing organizations...");
		Map<String, Organization> orgMap = new HashMap<>();
				
		entitymanager.getTransaction().begin();
			
		int numOrganizations = 0;	
		
		// read CSV file line by line
		for(int i=0; i<this.data.size(); i++) {
        	if ((i % 1000) == 0) {
        		System.out.println(i);
        	}
        	
        	// CSV data row
        	Map<String, String> orgData = this.data.get(i);
        	
        	Organization org = null;
        	String label1 = orgData.get("FIRMA1");
        	String label2 = orgData.get("FIRMA2"); 
        	String fullLabel = null;        	
    		if (label1 != null && !label1.equals("") && label2 != null && !label2.equals("")) {
    			fullLabel = label1 + " " + label2;    
    			fullLabel = stripAccents(fullLabel);
    			fullLabel = getCleanedString(fullLabel);
    			if (orgMap.containsKey(fullLabel)) {
    				org = orgMap.get(fullLabel);
    				
				} else {
					org = setOrganizationFields(orgData, sources);					
				}
    		}        	        	
        	
        	if (org != null) {
	        	
        		List<OrganizationIdentifier> orgIds = setOrganizationIdentifiers(orgData, org);
            	if (orgIds != null && orgIds.size() > 0) {
            		List<OrganizationIdentifier> previousOrgIds = org.getOrganizationIdentifiers(); 
            		if (previousOrgIds != null) {
            			for (OrganizationIdentifier newOrgId: orgIds) {
	            			boolean insert = true;
	            			for (OrganizationIdentifier oldOrgId : previousOrgIds) {
	            				if(oldOrgId.getIdentifier().equals(newOrgId.getIdentifier()) && oldOrgId.getIdentifierName().equals(newOrgId.getIdentifierName())) {
	            					insert = false;
	            					break;
	            				}
	            			}
	            			if (insert) {
	            				previousOrgIds.add(newOrgId);
	            			}
            			}
            			org.setOrganizationIdentifiers(previousOrgIds);
            		} else {
            			org.setOrganizationIdentifiers(orgIds);
            		}
            	}
	        	
	        	// connect organization with projects
	        	connectOrgWithProjects(entitymanager, orgData, org, prjMap);
	        	
	        	// connect organization with people
	        	connectOrgWithPeople(entitymanager, orgData, org, personMap);
				
	        	if (fullLabel != null) {
	        		orgMap.put(fullLabel, org); 	        		        		
	        	}
        	}
        	
		}
		
		for (Organization org: orgMap.values()) {
			numOrganizations+=1;
			if (!entitymanager.contains(org)) {
				entitymanager.persist(org);
			}
		}
		
		entitymanager.getTransaction().commit();
		
		System.out.println("Imported " + numOrganizations + " organizations.");
		
	}
	
	
	public void importData(String db) {
				
		// initialize entity manager factory
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();
						        
        // get or create aramis source -------------------------------------------
        entitymanager.getTransaction().begin();
        Query query = entitymanager.createQuery("Select s FROM Source s where s.label = :label");
        query.setParameter("label", this.sourceName);
        List<Source> aramisSource = query.getResultList();
        Source source = null;
        if (aramisSource.size() > 0) {
        	source = aramisSource.get(0);
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
    	
    	this.data = createMapFromFileCSV(dataReader, dataSchema);
    	
    	// import projects 
    	Map<String, Thematic> thematicMap = new HashMap<>();    	
    	Map<String, Project> prjMap = importProjects(entitymanager, sources, thematicMap);
    	
    	// import people
    	Map<String, Person> personMap = importPeople(entitymanager, sources);

    	// import organizations
    	importOrganizations(entitymanager, sources, prjMap, personMap);    	    	
    	    	    	
    	entitymanager.close();
		emfactory.close(); 
		
		System.out.println("Data import successfully completed.");
        
	}

}
