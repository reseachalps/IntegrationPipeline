package it.unimore.alps.sources.cnr;

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

public class CnrImporter {

	private Reader instReader;												// CNR institute file reader										
	private Reader depReader;												// CNR department file reader
	private CSVReader rootOrgReader;										// CNR headquarter file reader
	List<String> rootOrgSchema = new ArrayList<String>();					// CNR headquarter file schema
	List<Map<String, String>> rootOrgData = new ArrayList<>();				// CNR headquarter data
	private String sourceName = "Consiglio Nazionale delle Ricerche (CNR)";	// data source name
	private String sourceUrl = "https://www.cnr.it/";						// data source url
	private String sourceRevisionDate = "10-01-2018";						// data source date

	public static void main(String[] args) {

		// BEGIN: INPUT PARAMETERS ------------------------------------------------------
		CommandLine commandLine;
		// CNR headquarter file
		Option parentCNRFileOption = Option.builder("parentCNRFile").hasArg().required(true)
				.desc("The file that contains CNR root organization. ").longOpt("parentCNRFile").build();
		// CNR institute file
		Option instituteFileOption = Option.builder("instituteFile").hasArg().required(true)
				.desc("The file that contains CNR institutes data. ").longOpt("instituteFile").build();
		// CNR department file
		Option departmentFileOption = Option.builder("departmentFile").hasArg().required(true)
				.desc("The file that contains CNR departments data. ").longOpt("departmentFile").build();
		// database where to import data
		Option DB = Option.builder("DB").hasArg().required(true).desc("DB. ").longOpt("DB").build();

		Options options = new Options();
		CommandLineParser parser = new DefaultParser();

		options.addOption(instituteFileOption);
		options.addOption(departmentFileOption);
		options.addOption(parentCNRFileOption);
		options.addOption(DB);

		String instituteFile = null;
		String departmentFile = null;
		String parentCNRFile = null;
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
			
			if (commandLine.hasOption("parentCNRFile")) {
				parentCNRFile = commandLine.getOptionValue("parentCNRFile");
				System.out.println("\tCNR root organization file: " + parentCNRFile);
			} else {
				System.out.println("\tCNR root organization file not provided. Use the parentCNRFile option.");
				System.exit(1);
			}

			if (commandLine.hasOption("instituteFile")) {
				instituteFile = commandLine.getOptionValue("instituteFile");
				System.out.println("\tCNR institutes data file: " + instituteFile);
			} else {
				System.out.println("\tCNR institutes data file not provided. Use the instituteFile option.");
				System.exit(1);
			}
			
			if (commandLine.hasOption("departmentFile")) {
				departmentFile = commandLine.getOptionValue("departmentFile");
				System.out.println("\tCNR departments data file: " + departmentFile);
			} else {
				System.out.println("\tCNR departments data file not provided. Use the departmentFile option.");
				System.exit(1);
			}

			System.out.println("----------------------------\n");

		} catch (org.apache.commons.cli.ParseException e) {
			e.printStackTrace();
		}
		// END: INPUT PARAMETERS --------------------------------------------------------

		// import data
		System.out.println("Starting importing CNR data...");
		CnrImporter cnrImporter = new CnrImporter(parentCNRFile, instituteFile, departmentFile);
		cnrImporter.importData(db);

	}
	
	public CnrImporter(String parentCNRFile, String instituteFile, String departmentFile) {
		
		boolean header = true;
		this.rootOrgReader = initializeCSVReader(parentCNRFile, header, rootOrgSchema);
		this.rootOrgData = createMapFromFileCSV(rootOrgReader, rootOrgSchema);
		// read and load in memory the input CSV files
		try {
			this.instReader = new FileReader(instituteFile);
			this.depReader = new FileReader(departmentFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

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

		String label = (String) data.get("Denominazione");
		if (!label.equals("")) {
			org.setLabel(label);
		}

		JSONObject info = (JSONObject) data.get("Informazioni");
		String address = (String) info.get("Indirizzo");
		if (!address.equals("")) {
			org.setAddressSources(sources);
			org.setAddress(address);

			String pattern = "(\\d{5})";
			Pattern r = Pattern.compile(pattern);
			Matcher m = r.matcher(address);
			if (m.find()) {
				String postCode = m.group(0);
				org.setPostcode(postCode);
			}

		}

		String city = (String) info.get("sede");
		if (!city.equals("")) {
			String cleanCity = city.substring(0, 1).toUpperCase() + city.substring(1);
			org.setCity(cleanCity);
		}

		/*String urbanUnit = (String) info.get("provincia");
		if (!urbanUnit.equals("")) {
			org.setUrbanUnit(urbanUnit);
		}*/

		String acronym = (String) info.get("sigla");
		if (!acronym.equals("")) {
			List<String> acronyms = new ArrayList<>();
			acronyms.add(acronym);
			org.setAcronyms(acronyms);
		}

		org.setCountry("Italy");
		org.setCountryCode("IT");
		
		org.setIsPublic("true");

		return org;

	}

	private List<Leader> setOrganizationLeaders(JSONObject data) {

		// save organization leader information into Leader object
		List<Leader> leaders = new ArrayList<Leader>();

		Leader leader = new Leader();
		JSONObject leaderInfo = (JSONObject) data.get("Direttore");

		if (leaderInfo != null) {

			String email = (String) leaderInfo.get("E-mail");
			if (!email.equals("")) {
				leader.setEmail(email);
			}

			String fullName = (String) leaderInfo.get("Name");
			if (!fullName.equals("")) {
				List<String> fullNameWords = new ArrayList<String>(Arrays.asList(fullName.split(" ")));

				String lastName = fullNameWords.get(fullNameWords.size() - 1);
				leader.setLastName(lastName);

				fullNameWords.remove(fullNameWords.size() - 1);
				String firstName = String.join(" ", fullNameWords);
				leader.setFirstName(firstName);

			}

			leader.setTitle("Director");
			leaders.add(leader);
		}

		return leaders;
	}

	private Link setOrganizationLinks(JSONObject data) {

		// save organization website informantion into Link object
		Link link = new Link();

		JSONObject info = (JSONObject) data.get("Informazioni");
		if (info != null) {
			String url = (String) info.get("Url");

			if (!url.equals("")) {
				link.setUrl(url);
				link.setType("main");
				link.setLabel("homepage");
				return link;
			}
		}

		return null;
	}

	private OrganizationExtraField setExtraField(JSONObject data, String attrName, Organization org,
			List<OrganizationExtraField> orgExtraFields) {

		// save organization extra information into OrganizationExtraField object
		OrganizationExtraField orgExtraField = null;

		String attrVal = (String) data.get(attrName);
		if (!attrVal.equals("")) {
			orgExtraField = new OrganizationExtraField();
			orgExtraField.setVisibility(true);
			orgExtraField.setFieldKey(attrName);
			orgExtraField.setFieldValue(attrVal);
			orgExtraField.setOrganization(org);
			orgExtraFields.add(orgExtraField);
		}

		return orgExtraField;

	}

	private List<OrganizationExtraField> setOrganizationExtraFields(JSONObject data, Organization org) {
		
		// save organization extra information into OrganizationExtraField object
		List<OrganizationExtraField> orgExtraFields = new ArrayList<OrganizationExtraField>();

		JSONObject info = (JSONObject) data.get("Informazioni");
		if (info != null) {
			setExtraField(info, "E-mail", org, orgExtraFields);
			setExtraField(info, "Fax", org, orgExtraFields);
			setExtraField(info, "Telefono", org, orgExtraFields);
		}

		setExtraField(data, "Missione", org, orgExtraFields);
		setExtraField(data, "attivita", org, orgExtraFields);
		setExtraField(data, "collaborazioni", org, orgExtraFields);
		setExtraField(data, "competenze", org, orgExtraFields);
		setExtraField(data, "descrizione", org, orgExtraFields);
		setExtraField(data, "formazione", org, orgExtraFields);
		setExtraField(data, "servizi", org, orgExtraFields);

		return orgExtraFields;
	}
	
	private List<OrganizationExtraField> setChildOrganizationExtraFields(JSONObject data, Organization org) {
		List<OrganizationExtraField> orgExtraFields = new ArrayList<OrganizationExtraField>();

		setExtraField(data, "email", org, orgExtraFields);
		setExtraField(data, "fax", org, orgExtraFields);
		setExtraField(data, "regione", org, orgExtraFields);
		setExtraField(data, "telefono", org, orgExtraFields);

		return orgExtraFields;
	}
	
	public Organization setChildOrganizationFields(JSONObject childOrgData, List<Source> sources) {
		
		// save child organization information into Organization object
		Organization org = new Organization();

		String label = (String) childOrgData.get("istituto_denominazione_it");
		if (!label.equals("")) {
			org.setLabel(label);
		}
		String alias = (String) childOrgData.get("istituto_denominazione_en");
		if (alias != null && !alias.equals("")) {
			org.setAlias(alias);
		}

		String address = (String) childOrgData.get("indirizzo");
		if (!address.equals("")) {
			org.setAddressSources(sources);
			org.setAddress(address);
		}
		
		String postcode = (String) childOrgData.get("cap");
		if (!postcode.equals("")) {
			org.setPostcode(postcode);
		}

		String city = (String) childOrgData.get("citta");
		if (!city.equals("")) {
			String cleanCity = city.substring(0, 1).toUpperCase() + city.substring(1);
			org.setCity(cleanCity);
		}

		/*String urbanUnit = (String) childOrgData.get("provincia");
		if (!urbanUnit.equals("")) {
			org.setUrbanUnit(urbanUnit);
		}*/

		String acronym = (String) childOrgData.get("istituto_sigla");
		if (!acronym.equals("")) {
			List<String> acronyms = new ArrayList<>();
			acronyms.add(acronym);
			org.setAcronyms(acronyms);
		}

		org.setCountry("Italy");
		org.setCountryCode("IT");
		
		org.setIsPublic("true");
		
		return org;
	}
	
	private Organization setSubInstitute(JSONObject childOrgData, List<Source> sources, EntityManager entitymanager) {
		
		// set main organization fields
		Organization cOrg = setChildOrganizationFields(childOrgData, sources);
		
		// leaders
		Leader l = new Leader();
		String lastName = (String) childOrgData.get("congnome_resp");
		if (!lastName.equals("")) {
			l.setLastName(lastName);
		}
		String firstName = (String) childOrgData.get("nome_resp");
		if (!firstName.equals("")) {
			l.setFirstName(firstName);
		}
		l.setTitle("Responsible");
		l.setSources(sources);		
		entitymanager.persist(l);
		List<Leader> cLeaders = new ArrayList<>();
		cLeaders.add(l);
		if (cLeaders.size() > 0) {
			cOrg.setLeaders(cLeaders);
		}

		// connect the source to the related organization
		if (cOrg != null) {
			cOrg.setSources(sources);
		}
		
		// organization links
		Link cLink = null;
		String url = (String) childOrgData.get("indirizzo_internet");
		if (url != null && !url.equals("")) {
			cLink = new Link();
			cLink.setUrl(url);
			cLink.setType("main");
			cLink.setLabel("homepage");
		}
		
		if (cLink != null) {
			cLink.setSources(sources);
			entitymanager.persist(cLink);
			if (cOrg != null) {
				List<Link> orgLinks = new ArrayList<Link>();
				orgLinks.add(cLink);
				// connect the link to the related organization
				cOrg.setLinks(orgLinks);
			}
		}
		
		// set organization extra fields
		if (cOrg != null) {
			List<OrganizationExtraField> cOrgExtraFields = setChildOrganizationExtraFields(childOrgData, cOrg);
			for (OrganizationExtraField cOrgExtraField : cOrgExtraFields) {
				entitymanager.persist(cOrgExtraField);
			}
		}
		
		// set unique identifier
		if (cOrg != null) {
    	  	String identifier = sourceName + "::" + cOrg.getLabel() + "::" + cOrg.getAddress();
			OrganizationIdentifier orgId = new OrganizationIdentifier();
			orgId.setIdentifier(identifier);
			orgId.setProvenance(sourceName);
			orgId.setIdentifierName("lid");
			orgId.setOrganization(cOrg);
			entitymanager.persist(orgId);
		}
		
		return cOrg;
	}
	
	
	private List<OrganizationIdentifier> setOrganizationIdentifiers(JSONObject data, Organization org, String orgType, String instituteId) {
	  	 
		// save organization identifier information into OrganizationIdentifier object
	  	List<OrganizationIdentifier> orgIds = new ArrayList<OrganizationIdentifier>();
	  	
	  	OrganizationIdentifier orgId = null;
	  	
	  	String acr = null;
	  	JSONObject info = (JSONObject) data.get("Informazioni");
		if (info != null) {
		  	String acronym = (String) info.get("sigla");
			if (!acronym.equals("")) {
				acr = acronym.toLowerCase();
			}
		}
	  	
	  	String label = (String) data.get("Denominazione");
		if (!label.equals("")) {
			orgId = new OrganizationIdentifier();

			if (instituteId != null && acr != null) {

				Pattern p = Pattern.compile("(\\(.+\\))");
				Matcher m = p.matcher(label);
				if (m.find()) {
					String acronymInLabel = m.group(1);
					label = label.replace(" " + acronymInLabel, "");
				}

				label = label.toLowerCase() + " " + acr;
				label = label.replace("'", " ");
				String urlLabel = label.replace(" ", "-");
				String url = sourceUrl + "it/" + orgType + "/" + instituteId + "/" + urlLabel + "/"; 
				orgId.setLink(url);
				
				orgId.setIdentifier(instituteId);
		  		orgId.setProvenance(sourceName); 		
		  		orgId.setIdentifierName("CNR organization page");
		  		orgId.setOrganization(org);
		  		orgIds.add(orgId);	
			}
		}
	  	
	  	return orgIds;
	  	 
	}
	

	public Map<String, Organization> importInstitutes(EntityManager entitymanager, List<Source> sources) {
		Map<String, Organization> institutesMap = new HashMap<>();
		JSONParser parser = new JSONParser();

		// read JSON file
		try {

			Object fileObj = parser.parse(instReader);
			JSONArray orgsArray = (JSONArray) fileObj;
			Iterator<JSONObject> it = orgsArray.iterator();
			
			// read JSON file line by line
			while (it.hasNext()) {

				entitymanager.getTransaction().begin();
				
				// JSON data row
				JSONObject orgData = it.next();

				// set main organization fields
				Organization org = setOrganizationFields(orgData, sources);				

				// leaders
				List<Leader> leaders = setOrganizationLeaders(orgData);
				for (Leader l : leaders) {
					l.setSources(sources);					
					entitymanager.persist(l);
				}
				if (leaders.size() > 0) {
					org.setLeaders(leaders);
				}

				// connect the source to the related organization
				if (org != null) {
					org.setSources(sources);
				}

				// organization links
				Link link = setOrganizationLinks(orgData);
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

				// set organization extra fields
				if (org != null) {
					List<OrganizationExtraField> orgExtraFields = setOrganizationExtraFields(orgData, org);
					for (OrganizationExtraField orgExtraField : orgExtraFields) {
						entitymanager.persist(orgExtraField);
					}
				}
				
				// set unique identifier
				if (org != null) {
		    	  	String identifier = sourceName + "::" + org.getLabel() + "::" + org.getAddress();
					OrganizationIdentifier orgId = new OrganizationIdentifier();
					orgId.setIdentifier(identifier);
					orgId.setProvenance(sourceName);
					orgId.setVisibility(false);
					orgId.setIdentifierName("lid");
					orgId.setOrganization(org);
					entitymanager.persist(orgId);
				}
				
				
				String instituteId = null;
				
				// sub institutes
				List<Organization> childrenOrgs = new ArrayList<>();
				JSONObject childOrgsArray = (JSONObject) orgData.get("sezioni");
				for (Object key : childOrgsArray.keySet()) {
			        String childOrgKey = (String) key;
			        JSONObject childOrgData = (JSONObject) childOrgsArray.get(childOrgKey);
										
						Organization childOrg = setSubInstitute(childOrgData, sources, entitymanager);
						String childOrgId = (String) childOrgData.get("cds");
						if (!childOrgId.equals("") && childOrg != null) {
							instituteId = childOrgId;																					
							
						}
						entitymanager.persist(childOrg);
						childrenOrgs.add(childOrg);																												
						
				}
				
				if (org != null) {
	        		List<OrganizationIdentifier> orgIdentifiers = setOrganizationIdentifiers(orgData, org, "istituto", instituteId);
	        		for (OrganizationIdentifier orgIdentifier: orgIdentifiers) {
	        			entitymanager.persist(orgIdentifier);
	        		}
	        	}
				
				// institute identifier
				if (instituteId == null) {
					JSONObject info = (JSONObject) orgData.get("Informazioni");
					if (info != null) {
						String url = (String) info.get("Url");
						if ( url != null && !url.equals("") ) {
							instituteId = url;
						} else {
							String label = (String) orgData.get("Denominazione");
							if (!label.equals("")) {
								instituteId = url;
							}
						}
					}
				}
				
				if (org != null) {
					institutesMap.put(instituteId, org);	
					org.setChildrenOrganizations(childrenOrgs);				
					entitymanager.persist(org);
				}
				entitymanager.getTransaction().commit();

			}

			instReader.close();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return institutesMap;

	}
	
	private List<Person> setOrganizationPeople(JSONObject data) {

		// save person information into Person objects
		List<Person> people = new ArrayList<Person>();
		List<List<String>> peopleInfo = (List<List<String>>) data.get("consiglio-scientifico");
		int length = peopleInfo.size();
		if (length > 0) {
			for (int i = 0; i < length; i++) {
				Person person = new Person();
				JSONArray personInfo = (JSONArray) peopleInfo.get(i);
				int pLength = personInfo.size();
				if (pLength > 0) {
					String fullName = (String) personInfo.get(0);					
					String[] fullNameParts = fullName.split(" ");
					String lastName = fullNameParts[fullNameParts.length-1];
					ArrayList<String> firstNameParts = new ArrayList<>( Arrays.asList(fullNameParts) );
					firstNameParts.remove(firstNameParts.size()-1);
					String firstName = String.join(" ", firstNameParts);
					person.setFirstName(firstName);
					person.setLastName(lastName);
					
					String info = (String) personInfo.get(1);
					String[] infoParts = info.split("\n");
					String titlePart1 = infoParts[0].replace("Titolo: ", "");
					String titlePart2 = infoParts[1].replace("Afferenza: ", "");

					if (!titlePart1.equals("-") && !titlePart2.equals("-")) {
						person.setTitle(titlePart1 + " " + titlePart2); 
					} else {
						if (!titlePart1.equals("-")) {
							person.setTitle(titlePart1);
						}
					}
										
					people.add(person);
				}
			}
		}		

		return people;
	}
	
	public Map<Organization,List<String>> importDepartments(EntityManager entitymanager, List<Source> sources) {

		JSONParser parser = new JSONParser();
		Map<Organization,List<String>> depInstMap = new HashMap<>();

		// read JSON file
		try {

			Object fileObj = parser.parse(depReader);
			JSONArray orgsArray = (JSONArray) fileObj;
			Iterator<JSONObject> it = orgsArray.iterator();
			
			// read JSON file line by line
			while (it.hasNext()) {

				entitymanager.getTransaction().begin();

				// JSON data row
				JSONObject orgData = it.next();
				
				Organization org = new Organization();
				String label = (String) orgData.get("Denominazione");
				if (!label.equals("")) {
					org.setLabel(label);
				}
				JSONObject info = (JSONObject) orgData.get("Informazioni");
				if (info != null) {
					String address = (String) info.get("Indirizzo");
					if (!address.equals("")) {
						org.setAddress(address);
						org.setAddressSources(sources);
						
						String pattern = "(\\d{5})";
						Pattern r = Pattern.compile(pattern);
						Matcher m = r.matcher(address);
						if (m.find()) {
							String postCode = m.group(0);
							org.setPostcode(postCode);
						}																
					}											
					
					List<OrganizationExtraField> orgExtraFields = new ArrayList<OrganizationExtraField>();
					setExtraField(info, "Fax", org, orgExtraFields);
					setExtraField(info, "Telefono", org, orgExtraFields);
					for (OrganizationExtraField orgExtraField : orgExtraFields) {
						entitymanager.persist(orgExtraField);
					}
				}
				
				org.setCountry("Italy");
				org.setCountryCode("IT");
				
				// leaders
				List<Leader> leaders = setOrganizationLeaders(orgData);
				for (Leader l : leaders) {
					l.setSources(sources);										
					entitymanager.persist(l);
				}
				if (leaders.size() > 0) {
					org.setLeaders(leaders);
				}				
				
				// people
				List<Person> people = setOrganizationPeople(orgData);
				for (Person p : people) {
					p.setSources(sources);										
					entitymanager.persist(p);
				}
				if (people.size() > 0) {
					org.setPeople(people);
				}

				// connect the source to the related organization
				if (org != null) {
					org.setSources(sources);
					
					// set unique identifier
		    	  	String identifier = sourceName + "::" + org.getLabel() + "::" + org.getAddress();
					OrganizationIdentifier orgId = new OrganizationIdentifier();
					orgId.setIdentifier(identifier);
					orgId.setProvenance(sourceName);
					orgId.setIdentifierName("lid");
					orgId.setVisibility(false);
					orgId.setOrganization(org);
					entitymanager.persist(orgId);
				}			
				
				// organization links
				Link link = setOrganizationLinks(orgData);
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
		
				if (org != null) {
					entitymanager.persist(org);
				}
				
				// department institutes children
				JSONArray institutes = (JSONArray) orgData.get("instituti");
				int length = institutes.size();
				List<String> childInstitutes = new ArrayList<>();
				if (length > 0) {
					for (int i = 0; i < length; i++) {
						String instituteId = (String) institutes.get(i);
						if (instituteId != null && !instituteId.equals("")) {
							childInstitutes.add(instituteId);
						}
					}
				}
				
				if (childInstitutes != null && childInstitutes.size() > 0 && org != null) {
					depInstMap.put(org, childInstitutes);
				}
								
				entitymanager.getTransaction().commit();

			}

			depReader.close();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return depInstMap;

	}
	
	private Organization createParentOrganization(List<Source> sources, EntityManager entitymanager) {
		
		// read information about CNR headquarter from input CSV file and store it into Organization object
		entitymanager.getTransaction().begin();		
		Organization parentOrg = new Organization();
		
		// read CSV file line by line
		for(int i=0; i<rootOrgData.size(); i++) {									
			
			// CSV data row
        	Map<String, String> data = rootOrgData.get(i);
        	
			// label
        	String label = data.get("Denomination");
    		if (!label.equals("")) {
    			parentOrg.setLabel(label);
    		}    		
			
			// acronyms
			List<String> acronyms = new ArrayList<>();
			String acronym = data.get("Acronym");
    		if (!acronym.equals("")) {
    			acronyms.add(acronym);
    		}
			if (acronyms.size() > 0) {
				parentOrg.setAcronyms(acronyms);
			}
			
			// address
			String address = data.get("Address");
    		if (!address.equals("")) {
    			parentOrg.setAddressSources(sources);
		    	parentOrg.setAddress(address);
    		}
    		
    		// postcode
    		String postalCode = data.get("Postal code");
    		if (!postalCode.equals("")) {
    			parentOrg.setPostcode(postalCode);
    		}
    		
    		// city
    		String city = data.get("City");
    		if (!city.equals("")) {
    			String cleanCity = city.substring(0, 1).toUpperCase() + city.substring(1);
    			parentOrg.setCity(cleanCity);
    		}
    		
    		// country
    		String country = data.get("Country");
    		if (!country.equals("")) {
    			parentOrg.setCountry(country);
    		}
    		
    		// country code
    		String countryCode = data.get("Country Code");
    		if (!countryCode.equals("")) {
    			parentOrg.setCountryCode(countryCode);
    		}			
			
			// public organization
			parentOrg.setIsPublic("true");
			
			//source
			parentOrg.setSources(sources);
			
			// link
			String website = data.get("Website");
    		if (!website.equals("")) {    			
				Link link = new Link();
				link.setUrl(website); 
				link.setType("main");
				link.setLabel("homepage");
				List<Link> orgLinks = new ArrayList<Link>();
				orgLinks.add(link);
				parentOrg.setLinks(orgLinks);
    		}
    		
    		// organization identifier    	  	
    	  	OrganizationIdentifier orgId1 = null;
    	  	String orgIdentifier = data.get("Codice Fiscale");
    	  	if (!orgIdentifier.equals("")) {
    	  		orgId1 = new OrganizationIdentifier();
    	  		orgId1.setIdentifier(orgIdentifier);
    	  		orgId1.setProvenance(sourceName); 		
    	  		orgId1.setIdentifierName("Codice Fiscale");
    	  		orgId1.setOrganization(parentOrg);
    	  		entitymanager.persist(orgId1);
    	  	}
    	  	
    	  	OrganizationIdentifier orgId2 = null;
    	  	orgIdentifier = data.get("VAT code");
    	  	if (!orgIdentifier.equals("")) {
    	  		orgId2 = new OrganizationIdentifier();
    	  		orgId2.setIdentifier(orgIdentifier);
    	  		orgId2.setProvenance(sourceName); 		
    	  		orgId2.setIdentifierName("VAT code");
    	  		orgId2.setOrganization(parentOrg);
    	  		entitymanager.persist(orgId2);
    	  	}
    	  	
    	  	// set unique identifier
    	  	String identifier = sourceName + "::" + parentOrg.getLabel() + "::" + parentOrg.getAddress();
			OrganizationIdentifier orgId = new OrganizationIdentifier();
			orgId.setIdentifier(identifier);
			orgId.setProvenance(sourceName);
			orgId.setIdentifierName("lid");
			orgId.setVisibility(false);
			orgId.setOrganization(parentOrg);
			entitymanager.persist(orgId);
			
			entitymanager.persist(parentOrg);
		}

		entitymanager.getTransaction().commit();
		
		return parentOrg;
	}
	
	private void connectOrganizations(EntityManager entitymanager, Organization parentOrg, Map<String, Organization> institutesMap, Map<Organization,List<String>> depInstMap) {
			
		entitymanager.getTransaction().begin();
		
		// connect parent organization with institutes
		List<Organization> orgs = new ArrayList<>();
		orgs.addAll(institutesMap.values());
		parentOrg.setChildrenOrganizations(orgs);
		entitymanager.merge(parentOrg);

		// connect departments with institutes
		for (Organization dep: depInstMap.keySet()) {
			List<Organization> childInstitutes = new ArrayList<>();
			List<String> childInstituteIds = depInstMap.get(dep);
			if (childInstituteIds != null) {
				for (String childInstituteId: childInstituteIds) {
					Organization childInstitute = institutesMap.get(childInstituteId);
					if (childInstitute != null) {
						childInstitutes.add(childInstitute);
					}
				}
			}
			if (childInstitutes != null) {
				dep.setChildrenOrganizations(childInstitutes);
				entitymanager.merge(dep);
			}
		}
		
		entitymanager.getTransaction().commit();
	
	}

	public void importData(String db) {

		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();

		// get or create CNR source
		// ---------------------------------------------------------------
		entitymanager.getTransaction().begin();
		Query query = entitymanager.createQuery("Select s FROM Source s where s.label = :label");
		query.setParameter("label", sourceName);
		List<Source> cnrSource = query.getResultList();
		Source source = null;
		if (cnrSource.size() > 0) {
			source = cnrSource.get(0);
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

		Organization parentOrg = createParentOrganization(sources, entitymanager);
		Map<String, Organization> institutesMap = importInstitutes(entitymanager, sources);
		Map<Organization,List<String>> depInstMap = importDepartments(entitymanager, sources);
		connectOrganizations(entitymanager, parentOrg, institutesMap, depInstMap);
		System.out.println("Data import completed. Imported " + institutesMap.keySet().size() + " CNR institutes and " + depInstMap.keySet().size() + " departments.");
	}

}
