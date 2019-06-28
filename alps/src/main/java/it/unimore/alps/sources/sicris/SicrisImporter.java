package it.unimore.alps.sources.sicris;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.DateFormat;
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

public class SicrisImporter {
	
	private Reader orgReader;																		// organization file reader
	private Reader projectReader;																	// project file reader
	private Reader intProjectReader;																// international project file reader
	private Reader peopleReader;																	// people file reader
	List<String> orgSchema = new ArrayList<String>();												// organization file schema
	List<String> intProjectSchema = new ArrayList<String>();										// international project file schema
	List<String> projectSchema = new ArrayList<String>();											// project file schema
	List<String> peopleSchema = new ArrayList<String>();											// people file schema
	List<Map<String, String>> orgData = new ArrayList<>();											// organization data
	Map<String, Map<String, String>> projectData = new HashMap();									// project data
	Map<String, Map<String, String>> intProjectData = new HashMap();								// international project data
	Map<String, Map<String, String>> peopleData = new HashMap();									// people data
	private String sourceName = "SICRIS - Slovenian Current Research Information System";			// data source name
	private String sourceUrl = "http://www.sicris.si/public/jqm/cris.aspx?lang=eng&opdescr=home";	// data source url
	private String sourceRevisionDate = "05-10-2018";												// data source date
	
	
	public static void main(String[] args) {
		
		// BEGIN: INPUT PARAMETERS ------------------------------------------------------
		CommandLine commandLine;
		// organization file
        Option orgFileOption = Option.builder("orgFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains organization data. ")
	            .longOpt("organizationFile")
	            .build();   
        // project file
        Option prjFileOption = Option.builder("prjFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains project data.")
	            .longOpt("projectFile")
	            .build();
        // international project file
        Option intPrjFileOption = Option.builder("intPrjFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains international project data. ")
	            .longOpt("internationalProjectFile")
	            .build();
        // people file
        Option peopleFileOption = Option.builder("peopleFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains researchers data.")
	            .longOpt("peopleFile")
	            .build();
        // database where to import data
        Option DB = Option.builder("DB").hasArg().required(true).desc("DB. ").longOpt("DB").build();
        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addOption(orgFileOption);
        options.addOption(intPrjFileOption);
        options.addOption(prjFileOption);
        options.addOption(peopleFileOption);
        options.addOption(DB);
        
        String orgFile = null;
        String prjFile = null;
        String intPrjFile = null;
        String peopleFile = null;
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

	        if (commandLine.hasOption("orgFile")) {   
	        	orgFile = commandLine.getOptionValue("orgFile");
	        	System.out.println("\tSicris organization file: " + orgFile);
			} else {
				System.out.println("\tSicris organization file not provided. Use the orgFile option.");
	        	System.exit(1);
			}
	        
	        if (commandLine.hasOption("prjFile")) {   
	        	prjFile = commandLine.getOptionValue("prjFile");
	        	System.out.println("\tSicris project file: " + prjFile);
			} else {
				System.out.println("\tSicris project file not provided. Use the prjFile option.");
	        	System.exit(1);
			}
			
			if (commandLine.hasOption("intPrjFile")) {
				intPrjFile = commandLine.getOptionValue("intPrjFile");
				System.out.println("\tSicris international project file: " + intPrjFile);
			} else {
				System.out.println("\tSicris international project file not provided. Use the intPrjFile option.");
	        	System.exit(1);
			}
	
			if (commandLine.hasOption("peopleFile")) {
				peopleFile = commandLine.getOptionValue("peopleFile");
				System.out.println("\tSicris people file: " + peopleFile);
			} else {
				System.out.println("\tSicris people file not provided. Use the peopleFile option.");
				System.exit(1);
			}  								
			
			System.out.println("----------------------------\n");
        
        } catch (org.apache.commons.cli.ParseException e) {
			e.printStackTrace();
		}
		// END: INPUT PARAMETERS --------------------------------------------------------
        
		// import data
        SicrisImporter sicrisImporter = new SicrisImporter(orgFile, prjFile, intPrjFile, peopleFile);
        sicrisImporter.importData(db);

	}	
	
	public SicrisImporter(String orgFile, String projectFile, String intPrjFile, String peopleFile) {
		
		try {
			this.orgReader = new FileReader(orgFile);
			this.projectReader = new FileReader(projectFile);
			this.intProjectReader = new FileReader(intPrjFile);
			this.peopleReader = new FileReader(peopleFile);
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
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return source;		
	}	
	
	private Organization setOrganizationFields(EntityManager entitymanager, JSONObject data, List<Source> sources, Map<String, OrganizationType> orgTypeMap, List<OrganizationExtraField> orgExtraFields) {
		
		// save organization information into Organization object
		Organization org = new Organization();

		String label = (String) data.get("NAME");
		if (label != null && !label.equals("")) {
			org.setLabel(label);
		}
		String city = (String) data.get("CITY");
		if (city != null) {
			String cleanCity = city.substring(0, 1).toUpperCase() + city.substring(1);
			org.setCity(cleanCity);
		}
			
		JSONArray addresses = (JSONArray) data.get("CONTACT");
		if (addresses != null) {
			Iterator<JSONObject> it = addresses.iterator();
			while (it.hasNext()) {
	
				JSONObject addressObject = it.next();
	
				String address = (String) addressObject.get("ADDR1");
				if (address != null) {
					org.setAddress(address);
				}
				String postalcode = (String) addressObject.get("POSTALCODE");
				if (postalcode != null) {
					org.setPostcode(postalcode);
				}
				String city1 = (String) addressObject.get("CITY");
				if (city1 != null) {
					String cleanCity = city1.substring(0, 1).toUpperCase() + city1.substring(1);
					org.setCity(cleanCity);
				}
				String country = (String) addressObject.get("COUNTRY");
				if (country != null) {
					org.setCountry(country);
				}
				
				org.setAddressSources(sources);
				
				/*String phone = (String) addressObject.get("TEL1");
				if (phone != null) {
					OrganizationExtraField orgExtraField = new OrganizationExtraField();
					orgExtraField.setFieldKey("TEL");
					orgExtraField.setFieldValue(phone);
					orgExtraField.setVisibility(false);
					orgExtraField.setOrganization(org);
					orgExtraFields.add(orgExtraField);
				}
				
				String fax = (String) addressObject.get("FAX");
				if (fax != null) {
					OrganizationExtraField orgExtraField = new OrganizationExtraField();
					orgExtraField.setFieldKey("FAX");
					orgExtraField.setFieldValue(fax);
					orgExtraField.setVisibility(false);
					orgExtraField.setOrganization(org);
					orgExtraFields.add(orgExtraField);
				}
				
				String email = (String) addressObject.get("EMAIL");
				if (email != null) {
					OrganizationExtraField orgExtraField = new OrganizationExtraField();
					orgExtraField.setFieldKey("EMAIL");
					orgExtraField.setFieldValue(email);
					orgExtraField.setVisibility(false);
					orgExtraField.setOrganization(org);
					orgExtraFields.add(orgExtraField);
				}*/
				
				
				break;
			}
		}
				
		String type = (String) data.get("STATFRM_DESCR");
		if (type != null && !type.equals("")) {
			OrganizationType orgType = null;
			if (orgTypeMap.get(type) != null) {
				orgType = orgTypeMap.get(type);
			} else {
				orgType = new OrganizationType();
				orgType.setLabel(type);
				entitymanager.persist(orgType);
				orgTypeMap.put(type, orgType);
			}
			org.setOrganizationType(orgType);
			
		}
		
		org.setCountry("Slovenia");
		org.setCountryCode("SI");
			
		
		return org;
		
	}
	
	private List<Link> setOrganizationLinks(JSONObject data, List<Source> sources) {
		
		// save organization website information into Link object
		List<Link> links = new ArrayList<>();
		
		JSONArray addresses = (JSONArray) data.get("CONTACT");
		if (addresses != null) {
			Iterator<JSONObject> it = addresses.iterator();
			while (it.hasNext()) {
	
				JSONObject addressObject = it.next();
	
				String url = (String) addressObject.get("URL");
				if (url != null) {
					Link link = new Link();
					if (!url.contains("http://") && !url.contains("https://") && !url.equals("") && !url.equals(" ")) {
			    		url = "http://" + url;
			    	}
			    	link.setUrl(url);
					link.setType("main");
					link.setLabel("homepage");				
					link.setSources(sources);
					
					boolean insert = true;
					for (Link l : links) {
						if (l.getLabel().equals(link.getLabel())) {
							insert = false;
							break;
						}
					}
					if (insert) {
						links.add(link);
					}
				}
			}
		}
		
		return links;
	}
	
	public List<OrganizationActivity> setOrganizationActivities(EntityManager entitymanager, JSONObject data, Map<String, OrganizationActivity> mapOrgAct) {
		
		// save organization activity information into OrganizationActivity objects
		List<OrganizationActivity> orgActivities = new ArrayList<OrganizationActivity>();
			
		JSONArray actsARRS = (JSONArray) data.get("FRASCATI");
		if (actsARRS != null) {

			Iterator<JSONObject> it = actsARRS.iterator();
			while (it.hasNext()) {
	
				JSONObject actARRS = it.next();
				
				String codePart1 = (String) actARRS.get("SCIENCE");
				String codePart2 = (String) actARRS.get("FIELD");
				String codePart3 = (String) actARRS.get("SUBFIELD");
				String fullCode = null;
				if (codePart1 != null && codePart2 != null && codePart3 != null) {
					fullCode = codePart1 + "." + codePart2 + "." + codePart3;
				}
				
				if (fullCode != null) {
					String label = (String) actARRS.get("FIL_DESCR");
					if (label != null) {
						OrganizationActivity orgAct = null;
						String actId = "ARRS_" + fullCode;
						if (mapOrgAct.get(actId) != null) {
							orgAct = mapOrgAct.get(actId);
						} else {
							orgAct = new OrganizationActivity();
							orgAct.setCode(fullCode);
							orgAct.setLabel(label);
							orgAct.setType("ARRS");
							mapOrgAct.put(actId, orgAct);
						}
						
						boolean insert = true;
						for (OrganizationActivity act: orgActivities) {
							if(act.getCode().equals(orgAct.getCode())) {
								insert = false;
								break;
							}
						}
						if (insert) {
							orgActivities.add(orgAct);
						}
					}
				}
			}
		}
		
		JSONArray actsCerif = (JSONArray) data.get("CERIF");
		if (actsCerif != null) {
			Iterator<JSONObject> itC = actsCerif.iterator();
			while (itC.hasNext()) {
	
				JSONObject actCerif = itC.next();
				
				String codePart1 = (String) actCerif.get("CODE1");
				String codePart2 = (String) actCerif.get("CODE2");
				String fullCode = null;
				if (codePart1 != null && codePart2 != null) {
					fullCode = codePart1 + codePart2;
				}
				
				if (fullCode != null) {
					String label = (String) actCerif.get("DESCR2");
					if (label != null) {
						OrganizationActivity orgAct = null;
						String actId = "CERIF_" + fullCode;
						if (mapOrgAct.get(actId) != null) {
							orgAct = mapOrgAct.get(actId);
						} else {
							orgAct = new OrganizationActivity();
							orgAct.setCode(fullCode);
							orgAct.setLabel(label);
							orgAct.setType("CERIF");
							mapOrgAct.put(actId, orgAct);
						}
						
						boolean insert = true;
						for (OrganizationActivity act: orgActivities) {
							if(act.getCode().equals(orgAct.getCode())) {
								insert = false;
								break;
							}
						}
						if (insert) {
							orgActivities.add(orgAct);
						}
					}
				}
			}
		}
		
		JSONArray actsCordis = (JSONArray) data.get("CORDIS");
		if (actsCordis != null) {
			Iterator<JSONObject> itCo = actsCordis.iterator();
			while (itCo.hasNext()) {
	
				JSONObject actCordis = itCo.next();
				
				String code = (String) actCordis.get("CODE1");
				
				if (code != null) {
					String label = (String) actCordis.get("DESCR1");
					if (label != null) {
						OrganizationActivity orgAct = null;
						String actId = "CORDIS_" + code;
						if (mapOrgAct.get(actId) != null) {
							orgAct = mapOrgAct.get(actId);
						} else {
							orgAct = new OrganizationActivity();
							orgAct.setCode(code);
							orgAct.setLabel(label);
							orgAct.setType("CORDIS");
							mapOrgAct.put(actId, orgAct);
						}
						
						boolean insert = true;
						for (OrganizationActivity act: orgActivities) {
							if(act.getCode().equals(orgAct.getCode())) {
								insert = false;
								break;
							}
						}
						if (insert) {
							orgActivities.add(orgAct);
						}
					}
				}
			}
		}
								
		return orgActivities;
	}
	
	private List<Leader> setOrganizationLeader(EntityManager entitymanager, JSONObject data, List<Source> sources) {
		
		// save organization leader information into OrganizationActivity object
		List<Leader> leaders = new ArrayList<>();
		
		String firstName = (String) data.get("DIR_LNAME");
		String lastName = (String) data.get("DIR_FNAME");
		String title = (String) data.get("DIRFUN");
		if (firstName != null && lastName != null && title != null) {
			Leader leader = new Leader();
			leader.setFirstName(firstName);
			leader.setLastName(lastName);
			leader.setTitle(title);
			leader.setSources(sources);
			leaders.add(leader);
		}		
		
		return leaders;
	}
	
	private List<OrganizationIdentifier> setOrganizationIdentifiers(EntityManager entitymanager, JSONObject data, List<Source> sources, Organization org) {
		
		// save organization identifier information into OrganizationIdentifier objects
		List<OrganizationIdentifier> orgIds = new ArrayList<>();
		
		String mstid = (String) data.get("MSTID");
		if (mstid != null && !mstid.equals("")) {
			OrganizationIdentifier orgId = new OrganizationIdentifier();
			orgId.setIdentifier(mstid);
			orgId.setIdentifierName("MSTID");
			orgId.setOrganization(org);
			orgId.setProvenance(this.sourceName);
			orgId.setVisibility(true);
			orgIds.add(orgId);
			
			OrganizationIdentifier uniqueOrgId = new OrganizationIdentifier();
			uniqueOrgId.setIdentifier(this.sourceName + "::" + mstid);
			uniqueOrgId.setIdentifierName("lid");
			uniqueOrgId.setOrganization(org);
			uniqueOrgId.setProvenance(this.sourceName);
			uniqueOrgId.setVisibility(false);
			orgIds.add(uniqueOrgId);
		}
		
		String orgid = (String) data.get("ORGID");
		if (orgid != null && !orgid.equals("")) {
			OrganizationIdentifier orgId = new OrganizationIdentifier();
			orgId.setIdentifier(orgid);
			orgId.setIdentifierName("ORGID");
			orgId.setOrganization(org);
			orgId.setProvenance(this.sourceName);
			orgId.setVisibility(true);
			orgIds.add(orgId);
		}
		
		String rsrid = (String) data.get("RSRID");
		if (rsrid != null && !rsrid.equals("")) {
			OrganizationIdentifier orgId = new OrganizationIdentifier();
			orgId.setIdentifier(rsrid);
			orgId.setIdentifierName("RSRID");
			orgId.setOrganization(org);
			orgId.setProvenance(this.sourceName);
			orgId.setVisibility(true);
			orgIds.add(orgId);
		}
		
		return orgIds;
	}
	
	private List<Person> connectPeopleWithOrg(EntityManager entitymanager, JSONObject data, Organization org, Map<String, Person> peopleMap) {
		
		// connect people with organizations
		List<Person> orgPeople = new ArrayList<>();
		
		JSONArray people = (JSONArray) data.get("EMPLOY");
		if (people != null) {
			Iterator<JSONObject> it = people.iterator();
			while (it.hasNext()) {
	
				JSONObject person = it.next();
				
				String personId = (String) person.get("MSTID");
				if (personId != null) {
					if (peopleMap.get(personId) != null) {
						Person p = peopleMap.get(personId);
						orgPeople.add(p);
					}
				}
				
			}
		}
		
		return orgPeople;
	}
	
	
	private List<Project> connectProjectsWithOrg(EntityManager entitymanager, JSONObject data, Organization org, Map<String, Project> prjMap, String projectField) {
		
		// connect organization with projects
		List<Project> orgProjects = new ArrayList<>();
		
		JSONArray projects = (JSONArray) data.get(projectField);
		if (projects != null) {
			Iterator<JSONObject> it = projects.iterator();
			while (it.hasNext()) {
	
				JSONObject project = it.next();
				
				String projectId = (String) project.get("PRJID");
				if (projectId != null) {
					if (prjMap.get(projectId) != null) {
						Project prj = prjMap.get(projectId);
						if (prj != null) {
							
							boolean insert = true;
							
							for (Project p: orgProjects) {
								if (p.getLabel().equals(prj.getLabel())) {
									insert = false;
									break;
								}
							}
							
							if (insert) {
								orgProjects.add(prj);
							}
						}
					}
				}
			}
		}
		
		return orgProjects;
	}
	
	public void importOrganizations(EntityManager entitymanager, List<Source> sources, Map<String, OrganizationType> orgTypeMap, Map<String, OrganizationActivity> mapOrgAct, Map<String, Person> peopleMap, Map<String, Project> prjMap, Map<String, Project> intPrjMap) {
		
		// import organization information into the database
		System.out.println("Starting importing organizations...");
		
		JSONParser parser = new JSONParser();
		entitymanager.getTransaction().begin();
		
		int numOrgs = 0;

		List<OrganizationExtraField> orgExtraFields = new ArrayList<>();
		
		// read JSON file
		try {

			Object fileObj = parser.parse(this.orgReader);
			JSONArray orgsArray = (JSONArray) fileObj;
			Iterator<JSONObject> it = orgsArray.iterator();
			
			// read JSON file line by line
			while (it.hasNext()) {

				if ((numOrgs % 100) == 0) {
					System.out.println(numOrgs);
				}
				
				// JSON data row
				JSONObject orgData = it.next();
				
				// set main organization fields
				Organization org = setOrganizationFields(entitymanager, orgData, sources, orgTypeMap, orgExtraFields);
				//org.setOrganizationExtraFields(orgExtraFields);
				
				// sources
				org.setSources(sources);
				
				// organization identifiers
				List<OrganizationIdentifier> orgIds = setOrganizationIdentifiers(entitymanager, orgData, sources, org);
				if (orgIds != null && orgIds.size() > 0) {
					org.setOrganizationIdentifiers(orgIds);
				}
				
				// links				
				List<Link> links = setOrganizationLinks(orgData, sources);
				if (links != null && links.size() > 0) {
					org.setLinks(links);
				}
				
				// set organization activities
				/*List<OrganizationActivity> orgActs = setOrganizationActivities(entitymanager, orgData, mapOrgAct);
				if (orgActs != null && orgActs.size() > 0) {
					org.setActivities(orgActs);
				}*/
				
				// People			
				List<Person> orgPeople = connectPeopleWithOrg(entitymanager, orgData, org, peopleMap);
				if (orgPeople != null && orgPeople.size() > 0) {
					org.setPeople(orgPeople);
				}
				
				// local projects
				List<Project> projects = connectProjectsWithOrg(entitymanager, orgData, org, prjMap, "PROJECTS");
				List<Project> orgProjects = org.getProjects();
				if (orgProjects != null && orgProjects.size() > 0) {
					orgProjects.addAll(projects);
					org.setProjects(orgProjects);
				} else {
					org.setProjects(projects);
				}
				
				// international projects
				List<Project> intProjects = connectProjectsWithOrg(entitymanager, orgData, org, intPrjMap, "INTERNATIONAL");
				orgProjects = org.getProjects();
				if (orgProjects != null && orgProjects.size() > 0) {
					orgProjects.addAll(intProjects);
					org.setProjects(orgProjects);
				} else {
					org.setProjects(intProjects);
				}
				
				// leader
				List<Leader> leaders = setOrganizationLeader(entitymanager, orgData, sources);
				if (leaders != null && leaders.size() > 0) {
					org.setLeaders(leaders);
				}			
				
				entitymanager.persist(org);
			
				numOrgs+=1;
			
			}
			
			orgReader.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (org.json.simple.parser.ParseException e) {
			e.printStackTrace();
		}
		
		entitymanager.getTransaction().commit();
		
		System.out.println("Imported " + numOrgs + " organizations.");
		
		
	}
	
	private Project setProjectFields(EntityManager entitymanager, JSONObject data, List<Source> sources) {
		
		// save project information into Project object
		Project project= new Project();

		String title = (String) data.get("TITLE");
		if (title != null && !title.equals("")) {
			project.setLabel(title);
		}
		
		String descr = (String) data.get("ABSTRACT");
		if (descr != null && !descr.equals("")) {
			project.setDescription(descr);
		}
		
		
		Date projectStartDate = null;
		DateFormat df = new SimpleDateFormat("dd.MM.yyyy");
		String startDate = (String) data.get("STARTDATE");
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
			
			String end = (String) data.get("ENDDATE");
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
		
		return project;
		
	}
	
	private List<ProjectIdentifier> setProjectIdentifiers(EntityManager entitymanager, JSONObject data, List<Source> sources, Project prj) {
		
		// save project identifier information into ProjectIdentifier objects
		List<ProjectIdentifier> prjIds = new ArrayList<>();
		
		String prjIdFirstPart = (String) data.get("MSTID_PRG");
		String prjIdSecondPart = (String) data.get("MSTID_SCIENCE");
		String prjIdThirdPart = (String) data.get("MSTID_CONTR");
		
		if (prjIdFirstPart != null && prjIdThirdPart != null) {
			
			if (prjIdSecondPart == null) {
				prjIdSecondPart = "";
			}
			
			String completePrjId = prjIdFirstPart + prjIdSecondPart + "-" + prjIdThirdPart;
			
			ProjectIdentifier prjId = new ProjectIdentifier();
			prjId.setIdentifier(completePrjId);
			prjId.setIdentifierName("Project code");
			prjId.setProject(prj);
			prjId.setProvenance(sources.get(0).getLabel());
			prjId.setVisibility(true);
	    	prjIds.add(prjId);	    	
	    
		}		
		
		String prjIdString = (String) data.get("PRJID");		
		if (prjIdString != null) {
			
			ProjectIdentifier prjId = new ProjectIdentifier();
			prjId.setIdentifier(prjIdString);
			prjId.setIdentifierName("Project id");
			prjId.setProject(prj);
			prjId.setProvenance(sources.get(0).getLabel());
			prjId.setVisibility(true);
	    	prjIds.add(prjId);	    	
	    
		}
		
		return prjIds;
		
	}
	
	private List<Thematic> setProjectThematics(EntityManager entitymanager, JSONObject data, Map<String, Thematic> thematicMap) {
		
		// save project thematic information into Thematic objects
		List<Thematic> prjThematics = new ArrayList<>();
		
		JSONArray actsARRS = (JSONArray) data.get("FRASCATI");
		if (actsARRS != null) {
			Iterator<JSONObject> it = actsARRS.iterator();
			while (it.hasNext()) {
	
				JSONObject actARRS = it.next();
				
				String codePart1 = (String) actARRS.get("SCIENCE");
				String codePart2 = (String) actARRS.get("FIELD");
				String codePart3 = (String) actARRS.get("SUBFIELD");
				String fullCode = null;
				if (codePart1 != null && codePart2 != null && codePart3 != null) {
					fullCode = codePart1 + "." + codePart2 + "." + codePart3;
				}
				
				if (fullCode != null) {
					String label = (String) actARRS.get("FIL_DESCR");
					if (label != null) {
						Thematic thematic = null;
						String thematicId = "ARRS_" + fullCode;
						if (thematicMap.get(thematicId) != null) {
							thematic = thematicMap.get(thematicId);
						} else {
							thematic = new Thematic();
							thematic.setCode(fullCode);
							thematic.setLabel(label);
							thematic.setClassificationSystem("ARRS");
							thematicMap.put(thematicId, thematic);
						}
						
						boolean insert = true;
						for (Thematic theme: prjThematics) {
							if(theme.getCode().equals(thematic.getCode())) {
								insert = false;
								break;
							}
						}
						if (insert) {
							prjThematics.add(thematic);
						}
					}
				}
			}
		}
		
		JSONArray actsCerif = (JSONArray) data.get("CERIF");
		if (actsCerif != null) {

			Iterator<JSONObject> itC = actsCerif.iterator();
			while (itC.hasNext()) {
	
				JSONObject actCerif = itC.next();
				
				String codePart1 = (String) actCerif.get("CODE1");
				String codePart2 = (String) actCerif.get("CODE2");
				String fullCode = null;
				if (codePart1 != null && codePart2 != null) {
					fullCode = codePart1 + codePart2;
				}
				
				if (fullCode != null) {
					String label = (String) actCerif.get("DESCR2");
					if (label != null) {
						Thematic thematic = null;
						String thematicId = "CERIF_" + fullCode;
						if (thematicMap.get(thematicId) != null) {
							thematic = thematicMap.get(thematicId);
						} else {
							thematic = new Thematic();
							thematic.setCode(fullCode);
							thematic.setLabel(label);
							thematic.setClassificationSystem("CERIF");
							thematicMap.put(thematicId, thematic);
						}
						
						boolean insert = true;
						for (Thematic theme: prjThematics) {
							if(theme.getCode().equals(thematic.getCode())) {
								insert = false;
								break;
							}
						}
						if (insert) {
							prjThematics.add(thematic);
						}
					}
				}
			}
		}
		
		JSONArray actsCordis = (JSONArray) data.get("CORDIS");
		if (actsCordis != null) {

			Iterator<JSONObject> itCo = actsCordis.iterator();
			while (itCo.hasNext()) {
	
				JSONObject actCordis = itCo.next();
				
				String code = (String) actCordis.get("CODE1");
				
				if (code != null) {
					String label = (String) actCordis.get("DESCR1");
					if (label != null) {
						Thematic thematic = null;
						String thematicId = "CORDIS_" + code;
						if (thematicMap.get(thematicId) != null) {
							thematic = thematicMap.get(thematicId);
						} else {
							thematic = new Thematic();
							thematic.setCode(code);
							thematic.setLabel(label);
							thematic.setClassificationSystem("CORDIS");
							thematicMap.put(thematicId, thematic);
						}
						
						boolean insert = true;
						for (Thematic theme: prjThematics) {
							if(theme.getCode().equals(thematic.getCode())) {
								insert = false;
								break;
							}
						}
						if (insert) {
							prjThematics.add(thematic);
						}
					}
				}
			}
		}
								
		return prjThematics;
		
	}
	
	private List<ProjectExtraField> setProjectExtraFields(JSONObject data, Set<String> attributes, Project prj) {
		
		// save project extra information into ProjectExtraField objects
		List<ProjectExtraField> prjExtraFields = new ArrayList<ProjectExtraField>();				
		
		for (String extraFieldKey: attributes) {
			String extraField = (String) data.get(extraFieldKey);
			if (extraField != null) {
				ProjectExtraField prjExtraField = new ProjectExtraField();
				prjExtraField.setVisibility(true);
				prjExtraField.setFieldKey(extraFieldKey);
				prjExtraField.setFieldValue(extraField);
				prjExtraField.setProject(prj);
				prjExtraFields.add(prjExtraField);
			}
		}
		
		return prjExtraFields;
	}
	
	public Map<String, Project> importProjects(Reader reader, EntityManager entitymanager, List<Source> sources, Map<String, Thematic> thematicMap) {
		
		// import project data into the database
		String projectType = "international";
		if (reader == projectReader) {
			projectType = "local";
		}
		System.out.println("Starting importing " + projectType + " projects...");
		
		JSONParser parser = new JSONParser();
		entitymanager.getTransaction().begin();
		
		Map<String, Project> prjMap = new HashMap<>();
		Set<String> extraFields = new HashSet<String>(Arrays.asList("FNAME", "LNAME", "KEYWS"));
		
		int numProjects = 0;
		
		// read JSON file
		try {

			Object filePrj = parser.parse(reader);
			JSONArray prjsArray = (JSONArray) filePrj;
			Iterator<JSONObject> it = prjsArray.iterator();
			
			// read JSON file line by line
			while (it.hasNext()) {
				
				if ((numProjects % 1000) == 0) {
					System.out.println(numProjects);
				}				

				// JSON data row
				JSONObject prjData = it.next();
				
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
				
							
				// project extrafields
				List<ProjectExtraField> prjExtraFields = setProjectExtraFields(prjData, extraFields, prj);
				if (prjExtraFields != null && prjExtraFields.size() > 0) {
					prj.setProjectExtraFields(prjExtraFields);
				}
				
				entitymanager.persist(prj);
				
				String prjId = (String) prjData.get("PRJID");		
				if (prjId != null && prj != null) {					
					prjMap.put(prjId, prj);  				    
				}
				
				numProjects+=1;
			
			}
			
			projectReader.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (org.json.simple.parser.ParseException e) {
			e.printStackTrace();
		}
		
		entitymanager.getTransaction().commit();
		
		System.out.println("Imported " + numProjects + " " + projectType +  " projects.");
		
		return prjMap;
		
	}
	
	private Person setPersonFields(EntityManager entitymanager, JSONObject data, List<Source> sources) {
		
		// save person information into Person object
		Person person = new Person();	

		String firstName = (String) data.get("FNAME");
		if (firstName != null && !firstName.equals("")) {
			person.setFirstName(firstName);
		}
		
		String lastName = (String) data.get("LNAME");
		if (lastName != null && !lastName.equals("")) {
			person.setLastName(lastName);
		}
		
		String role = (String) data.get("TYPE");
		String roleAbbrev = (String) data.get("ABBREV");
		if (role != null && !role.equals("") && roleAbbrev != null && !roleAbbrev.equals("")) {
			String fullRole = role + " (" + roleAbbrev + ")";
			person.setTitle(fullRole);
		}
		
		JSONArray contacts = (JSONArray) data.get("CONTACT");
		if (contacts != null) {
			Iterator<JSONObject> it = contacts.iterator();
			while (it.hasNext()) {
	
				JSONObject contact = it.next();
				
				if (contact.containsKey("EMAIL")) {
					String email = (String) contact.get("EMAIL");
					if (email != null && !email.equals("")) {
			    		person.setEmail(email);
			    	}
				}			
			}
		}
		
		return person;
		
	}
	
	private List<PersonIdentifier> setPersonIdentifiers(EntityManager entitymanager, JSONObject data, List<Source> sources, Person person) {
		
		// save person identifier information into PersonIdentifier objects
		List<PersonIdentifier> personIds = new ArrayList<>();
		
		String personId1 = (String) data.get("MSTID");		
		if (personId1 != null) {
			
			PersonIdentifier personId = new PersonIdentifier();
			personId.setIdentifier(personId1);
			personId.setIdentifierName("MSTID");
			personId.setPerson(person);
			personId.setProvenance(sources.get(0).getLabel());
			personId.setVisibility(true);
	    	personIds.add(personId);	    	
	    
		}
		
		String personId2 = (String) data.get("RSRID");		
		if (personId2 != null) {
			
			PersonIdentifier personId = new PersonIdentifier();
			personId.setIdentifier(personId2);
			personId.setIdentifierName("RSRID");
			personId.setPerson(person);
			personId.setProvenance(sources.get(0).getLabel());
			personId.setVisibility(true);
	    	personIds.add(personId);	    	
	    
		}
		
		return personIds;
		
	}
	
	public Map<String, Person> importPeople(EntityManager entitymanager, List<Source> sources) {
		
		// import people data into the database
		System.out.println("Starting importing people...");
		
		JSONParser parser = new JSONParser();
		entitymanager.getTransaction().begin();
		
		int numPeople = 0;
		
		Map<String, Person> peopleMap = new HashMap<>();
		Set<String> extraFields = new HashSet<String>(Arrays.asList("KEYWS"));			
		
		// read JSON file
		try {

			Object filePeople = parser.parse(this.peopleReader);
			JSONArray peopleArray = (JSONArray) filePeople;
			Iterator<JSONObject> it = peopleArray.iterator();
			
			// read JSON file line by line
			while (it.hasNext()) {

				if ((numPeople % 1000) == 0) {
					System.out.println(numPeople);
				}
				
				// JSON data row
				JSONObject personData = it.next();
				
				// set main person fields
				Person person = setPersonFields(entitymanager, personData, sources);
				
				// sources
				person.setSources(sources);
				
				// person identifiers				
				List<PersonIdentifier> personIds = setPersonIdentifiers(entitymanager, personData, sources, person);
				if (personIds != null && personIds.size() > 0) {
					person.setPersonIdentifiers(personIds);
				}														
				
				entitymanager.persist(person);
				
				String personId = (String) personData.get("MSTID");		
				if (personId != null && person != null) {					
					peopleMap.put(personId, person);  				    
				}
				
				numPeople+=1;
			
			}
			
			projectReader.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (org.json.simple.parser.ParseException e) {
			e.printStackTrace();
		}
		
		entitymanager.getTransaction().commit();
		
		System.out.println("Imported " + numPeople + " people.");
		
		return peopleMap;
		
	}
	
	public void importData(String db) {
				
		// initialize entity manager factory
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();
						        
        // get or create sicris source -------------------------------------------
        entitymanager.getTransaction().begin();
        Query query = entitymanager.createQuery("Select s FROM Source s where s.label = :label");
        query.setParameter("label", this.sourceName);
        List<Source> sicrisSource = query.getResultList();
        Source source = null;
        if (sicrisSource.size() > 0) {
        	source = sicrisSource.get(0);
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
        		mapOrgAct.put(act.getType()+"_" + act.getCode(), act);
        	}
        }
        
        Map<String, Thematic> thematicMap = new HashMap<>();
        // local projects
    	Map<String, Project> prjMap = importProjects(projectReader, entitymanager, sources, thematicMap);
    	// international projects
    	Map<String, Project> intPrjMap = importProjects(intProjectReader, entitymanager, sources, thematicMap);
    	//people
    	Map<String, Person> peopleMap = importPeople(entitymanager, sources);    	
    	// organizations
    	importOrganizations(entitymanager, sources, orgTypeMap, mapOrgAct, peopleMap, prjMap, intPrjMap);
    	//entitymanager.getTransaction().commit();
    	entitymanager.close();
		emfactory.close(); 
		
		System.out.println("Data import successfully completed.");
        
	}

}
