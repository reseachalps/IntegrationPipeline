package it.unimore.alps.sources.cercauniversita;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
//import org.apache.commons.text.similarity.CosineSimilarity;
import org.simmetrics.metrics.CosineSimilarity;
import org.simmetrics.StringMetric;
import org.simmetrics.builders.StringMetricBuilder;
import org.simmetrics.metrics.StringMetrics;
import org.simmetrics.simplifiers.Simplifiers;
import org.simmetrics.tokenizers.Tokenizers;

import com.opencsv.CSVReader;

import it.unimore.alps.sources.arianna.AriannaImporter;
import it.unimore.alps.sql.model.Link;
import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationExtraField;
import it.unimore.alps.sql.model.OrganizationIdentifier;
import it.unimore.alps.sql.model.OrganizationType;
import it.unimore.alps.sql.model.Person;
import it.unimore.alps.sql.model.PersonExtraField;
import it.unimore.alps.sql.model.Project;
import it.unimore.alps.sql.model.ProjectExtraField;
import it.unimore.alps.sql.model.Source;

public class CercaUniversitaImporter {
	
	private CSVReader universityReader;										// university file reader
	private CSVReader departmentReader;										// department file reader
	private CSVReader instituteReader;										// institute file reader
	private CSVReader centreReader;											// research center file reader
	private CSVReader profReader;											// professor file reader
	private CSVReader fellowReader;											// research fellow file reader
	List<String> universitySchema = new ArrayList<String>();				// university file schema
	List<String> departmentSchema = new ArrayList<String>();				// department file schema
	List<String> instituteSchema = new ArrayList<String>();					// institute file schema
	List<String> centreSchema = new ArrayList<String>();					// research center file schema
	List<String> profSchema = new ArrayList<String>();						// professor file schema
	List<String> fellowSchema = new ArrayList<String>();					// research fellow file schema
	List<Map<String, String>> universityData = new ArrayList<>();			// university data
	List<Map<String, String>> departmentData = new ArrayList<>(); 			// department data
	List<Map<String, String>> instituteData = new ArrayList<>();			// institute data
	List<Map<String, String>> centreData = new ArrayList<>();				// research center data
	List<Map<String, String>> profData = new ArrayList<>();					// professor data
	List<Map<String, String>> fellowData = new ArrayList<>();				// research fellow data
	private String sourceName = "CercaUniversita";							// data source name
	private String sourceUrl = "http://cercauniversita.cineca.it/";			// data source url
	private String sourceRevisionDate = "01-06-2019";						// data source date
	
	public static void main(String[] args) {
		
		// BEGIN: INPUT PARAMETERS ------------------------------------------------------
		CommandLine commandLine;
		// university file
        Option uniFileOption = Option.builder("uniFile")
        		.hasArg()
        		.required(true)
        		.desc("The file that contains universities data. ")
        		.longOpt("universityFile")
        		.build();
		// department file
        Option depFileOption = Option.builder("depFile")
        		.hasArg()
        		.required(true)
        		.desc("The file that contains departments data.")
        		.longOpt("departmentFile")
        		.build();
		// institute file
        Option instituteFileOption = Option.builder("instituteFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains institutions data.")
	            .longOpt("instituteFile")
	            .build();
		// research center file
        Option centreFileOption = Option.builder("centreFile")
        		.hasArg()
	            .required(false)
	            .desc("The file that contains research centres data.")
	            .longOpt("centreFile")
	            .build();
		// professor file
        Option profFileOption = Option.builder("profFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains professors data.")
	            .longOpt("profFile")
	            .build();
		// research fellow file
        Option fellowFileOption = Option.builder("fellowFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains fellows data.")
	            .longOpt("fellowFile")
	            .build();
        Option DB = Option.builder("DB").hasArg().required(true).desc("DB. ").longOpt("DB").build();
        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addOption(uniFileOption);
        options.addOption(depFileOption);
        options.addOption(instituteFileOption);
        options.addOption(centreFileOption);
        options.addOption(profFileOption);
        options.addOption(fellowFileOption);
        options.addOption(DB);
        
        String universityFile = null;
        String departmentFile = null;
        String instituteFile = null;
        String centreFile = null;
        String profFile = null;
        String fellowFile = null;
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
			
	        if (commandLine.hasOption("uniFile")) {   
	        	universityFile = commandLine.getOptionValue("uniFile");
	        	System.out.println("\tUniversities file: " + universityFile);
			} else {
				System.out.println("\tUniversities file not provided. Use the uniFile option.");
	        	System.exit(1);
			}
			
			if (commandLine.hasOption("depFile")) {
				departmentFile = commandLine.getOptionValue("depFile");
				System.out.println("\tDepartments file: " + departmentFile);
			} else {
				System.out.println("\tDepartments file not provided. Use the depFile option.");
	        	System.exit(1);
			}
	
			if (commandLine.hasOption("instituteFile")) {
				instituteFile = commandLine.getOptionValue("instituteFile");
				System.out.println("\tInstitutions file: " + instituteFile);
			} else {
				System.out.println("\tInstitutions file not provided. Use the instituteFile option.");
				System.exit(1);
			}  
			
			if (commandLine.hasOption("centreFile")) {
				centreFile = commandLine.getOptionValue("centreFile");	
				System.out.println("\tResearch centre file: " + centreFile);
			} else {
				System.out.println("\tResearch centres file not provided. Use the centreFile option.");
				System.exit(1);
			}
			
			if (commandLine.hasOption("profFile")) {
				profFile = commandLine.getOptionValue("profFile");	
				System.out.println("\tProfessor file: " + profFile);
			} else {
				System.out.println("\tProfessor file not provided. Use the profFile option.");
				System.exit(1);
			}
			
			if (commandLine.hasOption("fellowFile")) {
				fellowFile = commandLine.getOptionValue("fellowFile");	
				System.out.println("\tFellow file: " + fellowFile);
			} else {
				System.out.println("\tFellow file not provided. Use the fellowFile option.");
				System.exit(1);
			}
			
			System.out.println("----------------------------\n");
        
        } catch (org.apache.commons.cli.ParseException e) {
			e.printStackTrace();
		}
		// END: INPUT PARAMETERS --------------------------------------------------------
        
        // import data
        System.out.println("Starting importing cercauniversita data...");
        CercaUniversitaImporter cercauniversitaImporter = new CercaUniversitaImporter(universityFile, departmentFile, instituteFile, centreFile, profFile, fellowFile, header);
		cercauniversitaImporter.importData(db);

	}
	
	public CercaUniversitaImporter(String universityFile, String departmentFile, String instituteFile, String centreFile, String profFile, String fellowFile, boolean header) {
		// read and load in memory input CSV files
		this.universityReader = initializeCSVReader(universityFile, header, universitySchema);
		this.departmentReader = initializeCSVReader(departmentFile, header, departmentSchema);
		this.instituteReader = initializeCSVReader(instituteFile, header, instituteSchema);
		this.centreReader = initializeCSVReader(centreFile, header, centreSchema);
		this.profReader = initializeCSVReader(profFile, header, profSchema);
		this.fellowReader = initializeCSVReader(fellowFile, header, fellowSchema);
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
	
	private Organization setOrganizationFields(Map<String,String> data, String keyField, List<Source> sources) {
		
		// save organization information into Organization object 
		Organization org = new Organization();
		
    	String label = data.get(keyField);
		if (!label.equals("")) {
			org.setLabel(label);
		}
	    
	    String address = data.get("Via");
	    if (!address.equals("") && !address.contains("?")) {
	    	org.setAddressSources(sources);
	    	org.setAddress(address);
	    }
	    
	    String cap = data.get("Cap");
	    if (!cap.equals("")) {
	    	org.setPostcode(cap);
	    }
	    
	    String city = data.get("Città");
	    if (!city.equals("")) {
	    	String cleanCity = city.substring(0, 1).toUpperCase() + city.substring(1);
	    	org.setCity(cleanCity);
	    }
	    
	    org.setCountry("Italy");
	    org.setCountryCode("IT");
	    
	    String isPrivate = data.get("Privata");
	    if (isPrivate != null) {
		    if (!isPrivate.equals("")) {
		    	org.setIsPublic("false");
		    } else {
		    	org.setIsPublic("true");
		    }
	    }
	    
	    return org;
		
	}
	
	private Link setOrganizationLinks(Map<String,String> data, String websiteKey) {
		
		// save organization website information into Link object
		Link link = null;
		
		String website = data.get(websiteKey); 
		if (!website.equals("")) {
			link = new Link();
	    	link.setUrl(website);
	    	link.setType("main");
	    	link.setLabel("homepage");
	    }
	    
	    return link;
	}
	
	private List<OrganizationExtraField> setOrganizationExtraFields(Map<String,String> data, Set<String> attributes, Organization org) {
		
		// save organization extra information into OrganizationExtraField objects
		List<OrganizationExtraField> orgExtraFields = new ArrayList<OrganizationExtraField>();
		
		for (String key : data.keySet()) {
			if (attributes.contains(key)) {
				if (!data.get(key).equals("")) {
					OrganizationExtraField orgExtraField = new OrganizationExtraField();
					orgExtraField.setFieldKey(key);
					orgExtraField.setVisibility(true);
					orgExtraField.setFieldValue(data.get(key));
					orgExtraField.setOrganization(org);
					orgExtraFields.add(orgExtraField);
				}
			}
		}
		
		return orgExtraFields;
	}
	
	private Map<String, List<Organization>> saveData(List<Map<String, String>> data, EntityManager entitymanager, String keyField, String websiteKey, String organizationType, List<Source> sources, Set<String> attributes, Set<String> attributesExtra, String parentKey) {		
		
		Map<String, List<Organization>> parentChildMap = new HashMap<String, List<Organization>>();
		entitymanager.getTransaction().begin();		
		
		Organization precOrg = null;
		String parentOrgKey = null;
		
		// read CSV file line by line
	    for(int i=0; i<data.size(); i++) {
	    	
	    	if ((i % 100) == 0) {
        		System.out.println(i);
        	}	    	
	    	
	    	Map<String, String> rowData = data.get(i);
	    	
	    	// the parent-child organization relationship is structured in the input CSV files in this way:
	    	// parent organization - child organization 1 (parent + child organizations)
	    	//					   - child organization 2 (only child organization)
	    	String valueKeyField = rowData.get(keyField);
	    	if (!valueKeyField.equals("")) {											// parent + child organizations
	    		
	    		// set main organization fields
	        	Organization org = setOrganizationFields(rowData, keyField, sources);	        	
	        
	        	if (org != null) {
	        		org.setTypeLabel(organizationType);
	        	}
	        	
	        	// organization links
	        	Link link = setOrganizationLinks(rowData, websiteKey);
	        	if (link != null) {
	        		link.setSources(sources);
	        		entitymanager.persist(link);
	        		if (org != null) {
	        			// connect the link to the related organization
	        			List<Link> orgLinks = new ArrayList<Link>();
	        			orgLinks.add(link);
	        			org.setLinks(orgLinks);
	        		}
	        	}
	        	
	        	// connect the source to the related organization
	        	if (org != null) {
	        		org.setSources(sources);	        		        	
	        	}
	
	        	// set unique identifier
	        	if (org != null) {	        		
	        		String identifier = sourceName + "::" + org.getLabel() + "::" + org.getAddress() + "::" + org.getCity();
					OrganizationIdentifier orgId = new OrganizationIdentifier();
					orgId.setIdentifier(identifier);
					orgId.setProvenance(sourceName);
					orgId.setIdentifierName("lid");
					orgId.setVisibility(false);
					orgId.setOrganization(org);
					entitymanager.persist(orgId);	        		
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
	        	
	        	// assigning the same parent organization to all children organizations
	        	precOrg = org;
	        	
	        	if (!rowData.get(parentKey).equals("")) {
	        		if (org != null) {
	        			List<Organization> orgs = new ArrayList<Organization>();
	        			orgs.add(org);
	        			parentChildMap.put(rowData.get(parentKey), orgs);
	        			parentOrgKey = rowData.get(parentKey);
	        		}
	        	} else {
	        		if (org != null) {
	        			parentChildMap.get(parentOrgKey).add(org);
	        		}
	        	}
	    		
	    	} else {				// only child organization (information deriving from file Atenei.csv)
	    		
	    		// set organization extra fields
	        	if (precOrg != null) {
	        		List<OrganizationExtraField> orgExtraFields = setOrganizationExtraFields(rowData, attributesExtra, precOrg);
	        		for (OrganizationExtraField orgExtraField: orgExtraFields) {
	        			entitymanager.persist(orgExtraField);
	        		}
	        	}
	        	
	        	entitymanager.merge(precOrg);
	    	}	    	
	    }
	    
	    entitymanager.getTransaction().commit();
	    return parentChildMap;
	}
	
	private void connectParentChildrenOrganizations(Map<String, List<Organization>> parentMap, Map<String, List<Organization>> childrenMap, EntityManager entitymanager) {
		
		// connect parent-children organizations
		entitymanager.getTransaction().begin();
		
		for (String parentKey: childrenMap.keySet()) {
			Organization org = parentMap.get(parentKey).get(0);
			List<Organization> newChildren = childrenMap.get(parentKey);
			List<Organization> oldChildren = org.getChildrenOrganizations();
			if (oldChildren != null) {
				for(Organization child: oldChildren) {
					newChildren.add(child);
				}
			}
			org.setChildrenOrganizations(newChildren);
			entitymanager.merge(org);
			
			for (Organization nChild: newChildren) {
				nChild.setIsPublic(org.getIsPublic());
				entitymanager.merge(nChild);
			}
		}
		
		entitymanager.getTransaction().commit();
	}
	
	public void connectPersonToOrganization(Person person, String personUni, String personDip, Map<String, List<Organization>> uniMap, EntityManager entitymanager) {
		
		// connecting people with organizations
		// the connection is made through a regular expression as the person and organization files
		// may use different textual mentions to refer to the same organization
		// in current implementation only first match is considered
		boolean found = false;
		StringMetric metric = StringMetrics.cosineSimilarity();
		boolean debug = false;
		
		Map<Integer,List<String>> mySortedMap = new TreeMap<Integer,List<String>>();
		for (String uniKey: uniMap.keySet()) {
			
			List<String> token_Uni = Arrays.asList(personUni.split(" "));
			
			int commonToken = 0;
			for (String token: token_Uni) {
				if (token.length()>3 && !token.toLowerCase().contains("univ") && Pattern.compile(Pattern.quote(token), Pattern.CASE_INSENSITIVE).matcher(uniKey).find()) {
					commonToken++;
				}
			}
			if (commonToken > 0) {
				List<String> unikeys = null;
				if (mySortedMap.get(commonToken) == null) {
					unikeys = new ArrayList<>();					
				} else {
					unikeys = mySortedMap.get(commonToken);
				}
				unikeys.add(uniKey);
				mySortedMap.put(commonToken, unikeys);
			}
		}
		
		if (mySortedMap.size() > 0) {
			Map<Integer,List<String>> mySortedMap1 = new TreeMap<Integer,List<String>>(new Comparator<Integer>() 
		     {
		        @Override
		        public int compare(Integer o1, Integer o2) {                
		            return o2.compareTo(o1);
		         }
		     });
			mySortedMap1.putAll(mySortedMap);
			mySortedMap = mySortedMap1;		
			
			List<String> unikeys = new ArrayList<>();
			for (Integer cT: mySortedMap.keySet()) {
				for (String s: mySortedMap.get(cT)) {
					unikeys.add(s);
				}
			}
			
			for (String uniKey: unikeys) {

				// accessing to university children organization information
				List<Organization> orgsChildren = uniMap.get(uniKey).get(0).getChildrenOrganizations();
				float maxScore = 0;
				Organization matchedOrg = null;
				// trying to match the department that matches with the professor department
				for (Organization orgChild: orgsChildren) {
										
					if (debug) {
						System.out.println("SIM: " + personDip + " -> " + orgChild.getLabel());
					}	
					float score = metric.compare(personDip.toLowerCase(), orgChild.getLabel().toLowerCase());
					if ( score > 0.7) {
						if (score > maxScore) {
							maxScore = score;
							matchedOrg = orgChild;
						}
					}
				}
				if (matchedOrg != null) {
									
					List<Person> people = matchedOrg.getPeople();
					if (people != null) {
						people.add(person);
						matchedOrg.setPeople(people);
					} else {
						List<Person> newPeople = new ArrayList<>();
						newPeople.add(person);
						matchedOrg.setPeople(newPeople);
					}
					entitymanager.merge(matchedOrg);
					found = true;
					// terminating the search if the match is exact
					break;
				}
			}
		}
	}
	
	private List<PersonExtraField> setPersonExtraFields(Map<String,String> data, Set<String> attributes, Person person) {
		
		// save person extra information into PersonExtraField objects
		List<PersonExtraField> pExtraFields = new ArrayList<PersonExtraField>();			
		
		for (String key : data.keySet()) {
			if (attributes.contains(key)) {
				if (!data.get(key).equals("")) {
					PersonExtraField pExtraField = new PersonExtraField();
					pExtraField.setVisibility(true);
					pExtraField.setFieldKey(key);
					pExtraField.setFieldValue(data.get(key));
					pExtraField.setPerson(person);
					pExtraFields.add(pExtraField);
				}
			}
		}
		
		return pExtraFields;
	}
	
	private void saveProfData(List<Map<String, String>> data, EntityManager entitymanager, Map<String, List<Organization>> uniMap, List<Source> sources) {
		
		// read organization data and import it into the database 
		System.out.println("Starting importing professor data...");
		entitymanager.getTransaction().begin();

		// read CSV file line by line
        for(int i=0; i<data.size(); i++) {
        	if ((i % 1000) == 0) {
        		System.out.println(i);
        	}
        	
        	// CSV data row
        	Map<String, String> rowData = data.get(i);
        	
        	Person prof = new Person();
        	
        	String title = rowData.get("Fascia");
    		if (!title.equals("")) {
    			prof.setTitle(title);
    		}
    		
    		String fullName = rowData.get("Cognome e Nome");
    		if (!fullName.equals("")) {
    			String[] fullNameItems = fullName.split(" ");
    			String lastName = fullNameItems[0];
    			
    			String firstName = String.join(" ", Arrays.asList(fullNameItems).subList(1, fullNameItems.length));
    			prof.setFirstName(firstName);
    			prof.setLastName(lastName);
    		}
    		prof.setSources(sources);
    		
    		if (prof != null) {
    			Set<String> attributes = new HashSet<String>(Arrays.asList("Genere", "S.S.D.", "S.C."));
        		List<PersonExtraField> pExtraFields = setPersonExtraFields(rowData, attributes, prof);
        		for (PersonExtraField pExtraField: pExtraFields) {
        			entitymanager.persist(pExtraField);
        		}
        	}    		
    		
    		entitymanager.persist(prof);
    		
    		String uni = rowData.get("Ateneo");
    		String dip = rowData.get("Struttura di afferenza");
    		if (!uni.equals("") && !dip.equals("")) {
    			
    			dip = dip.replace("Dip. ","");
    			connectPersonToOrganization(prof, uni, dip, uniMap, entitymanager);  
    			
    		}
        }
        
        entitymanager.getTransaction().commit();
        
	}
	
	public void saveFellowData(List<Map<String, String>> data, EntityManager entitymanager, Map<String, List<Organization>> uniMap, List<Source> sources) {
		
		// read organization data and import it into the database 
		System.out.println("Starting importing fellows data...");
		entitymanager.getTransaction().begin();

		// read CSV file line by line
        for(int i=0; i<data.size(); i++) {
        	if ((i % 1000) == 0) {
        		System.out.println(i);
        	}
        	
        	// CSV data row
        	Map<String, String> rowData = data.get(i);
        	
        	Person fellow = new Person();
        	
        	String title = rowData.get("Qualifica");
    		if (!title.equals("")) {
    			fellow.setTitle(title);
    		}
    		
    		String fullName = rowData.get("Cognome e Nome");
    		if (!fullName.equals("")) {
    			String[] fullNameItems = fullName.split(" ");
    			String lastName = fullNameItems[0];
    			String firstName = String.join(" ", Arrays.asList(fullNameItems).subList(1, fullNameItems.length));
    			fellow.setFirstName(firstName);
    			fellow.setLastName(lastName);
    		}
    		fellow.setSources(sources);
    		
    		if (fellow != null) {
    			Set<String> attributes = new HashSet<String>(Arrays.asList("Area","S.S.D.","Argomento della Ricerca")); 			
        		List<PersonExtraField> pExtraFields = setPersonExtraFields(rowData, attributes, fellow);
        		for (PersonExtraField pExtraField: pExtraFields) {
        			entitymanager.persist(pExtraField);
        		}
        	}   
    		
    		entitymanager.persist(fellow);
    		
    		String uni = rowData.get("Ateneo");
    		String dip = rowData.get("Dipartimento");
    		if (!uni.equals("") && !dip.equals("")) {
    			
    			dip = dip.replace("Dip. ","");
    			
    			connectPersonToOrganization(fellow, uni, dip, uniMap, entitymanager);    			    			    			
    		}       
        }
        
        entitymanager.getTransaction().commit();
	}
	
	public void importData(String db) {
		
		// Technical issue: the columns of the input CSV files have to be unique; If necessary change them manually. 
		
		// initialize entity manager factory
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();		
		
		this.universityData = createMapFromFileCSV(universityReader, universitySchema);
        this.departmentData = createMapFromFileCSV(departmentReader, departmentSchema);
        this.instituteData = createMapFromFileCSV(instituteReader, instituteSchema);
        this.centreData = createMapFromFileCSV(centreReader, centreSchema);
        this.profData = createMapFromFileCSV(profReader, profSchema);
        this.fellowData = createMapFromFileCSV(fellowReader, fellowSchema);
        
        entitymanager.getTransaction().begin();
		Source source = setSource();
        entitymanager.persist(source);
        List<Source> sources = new ArrayList<Source>();
    	sources.add(source);
    	entitymanager.getTransaction().commit();
		
    	// read organization data and import it into the database    	
    	Set<String> attributes = new HashSet<String>(Arrays.asList("Telefono", "Fax"));
    	Set<String> attributesExtra = new HashSet<String>(Arrays.asList("Via", "Città", "Cap"));

    	Map<String, List<Organization>> uniMap = saveData(universityData, entitymanager, "Ateneo", "Sito Web", "University", sources, attributes, attributesExtra, "Ateneo");
    	Map<String, List<Organization>> uniDepMap = saveData(departmentData, entitymanager, "Dipartimento", "Sito Dipartimento", "Department", sources, attributes, null, "Ateneo");
    	Map<String, List<Organization>> uniInstMap = saveData(instituteData, entitymanager, "Istituto", "Sito Istituto", "Institute", sources, attributes, null, "Ateneo");
    	Map<String, List<Organization>> uniCentreMap = saveData(centreData, entitymanager, "Centri", "Sito Centri", "Research centre", sources, attributes, null, "Ateneo");
    	
    	// connect children with parent entity
    	connectParentChildrenOrganizations(uniMap, uniDepMap, entitymanager);
    	connectParentChildrenOrganizations(uniMap, uniInstMap, entitymanager);
    	connectParentChildrenOrganizations(uniMap, uniCentreMap, entitymanager);
    	
    	saveProfData(profData, entitymanager, uniMap, sources);
    	saveFellowData(fellowData, entitymanager, uniMap, sources);
    	
    	
    	System.out.println("Imported " + uniMap.values().size() + " universities, " + uniDepMap.values().size() + " departments, " + uniInstMap.values().size() + " institutions and " + uniCentreMap.values().size() + " research centres.");
        
		entitymanager.close();
		emfactory.close();
	}     

}
