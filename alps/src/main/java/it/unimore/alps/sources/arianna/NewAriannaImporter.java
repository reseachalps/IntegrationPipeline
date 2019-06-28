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
import it.unimore.alps.sql.model.OrganizationActivity;
import it.unimore.alps.sql.model.OrganizationExtraField;
import it.unimore.alps.sql.model.OrganizationIdentifier;
import it.unimore.alps.sql.model.OrganizationType;
import it.unimore.alps.sql.model.Project;
import it.unimore.alps.sql.model.ProjectExtraField;
import it.unimore.alps.sql.model.ProjectIdentifier;
import it.unimore.alps.sql.model.Source;

import it.unimore.alps.idgenerator.LocalIdentifierGenerator;

public class NewAriannaImporter {
	
	private String orgFile;																					// organization file
	private String projectFile;																				// project file
	private String bindingFile;																				// organization-project file
	private String orgBRFile;																				// organization file (from Business Registry)
	private CSVReader orgReader;																			// organization file reader
	private CSVReader orgBrReader;																			// organization file reader (from Business Registry)
	private CSVReader projectReader;																		// project file reader
	private CSVReader bindingReader;																		// organization-project file reader
	List<String> orgSchema = new ArrayList<String>();														// organization file schema
	List<String> orgBrSchema = new ArrayList<String>();														// organization file schema (from Business Registry)
	List<String> projectSchema = new ArrayList<String>();													// project file schema
	List<String> bindingSchema = new ArrayList<String>();													// organization-project file schema
	List<Map<String, String>> orgData = new ArrayList<>();													// organization data
	Map<String, Map<String, String>> orgBrData = new HashMap();												// organization data (from Business Registry)
	List<Map<String, String>> projectData = new ArrayList<>(); 												// project data
	private String sourceName = "Arianna - Anagrafe Nazionale delle Ricerche";								// data source name
	private String sourceUrl = "http://www.anagrafenazionalericerche.it/arianna/contentpages/default.aspx";	// data source url
	private String sourceRevisionDate = "15-06-2018";														// data source date
	
	
	public static void main(String[] args) {
		
		// BEGIN: INPUT PARAMETERS ------------------------------------------------------
		CommandLine commandLine;
		// organization file
        Option orgFileArOption = Option.builder("orgFileArianna")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains organization data. ")
	            .longOpt("organizationFileArianna")
	            .build();
        // organization file from Business Registry
        Option orgFileBROption = Option.builder("orgFileBusinessRegistry")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains organization data. ")
	            .longOpt("organizationFileBusinessRegistry")
	            .build();
        // project file
        Option projectFileOption = Option.builder("prjFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains project data.")
	            .longOpt("projectFile")
	            .build();
        // organization-project file
        Option orgPrjFileOption = Option.builder("orgPrjFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains the links between organizations and projects.")
	            .longOpt("organizationProjectFile")
	            .build();
        // entity type to be imported (all, organization, project and link)
        Option entityTypeOption = Option.builder("entityType")
        		.hasArg()
	            .required(true)
	            .desc("The entity type to be imported. The possibile choiches are: 'all', 'organization', 'project' and 'link'.")
	            .longOpt("entityTypeImported")
	            .build();
        // database where to import data
        Option DB = Option.builder("DB").hasArg().required(true).desc("DB. ").longOpt("DB").build();
        
        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addOption(orgFileArOption);
        options.addOption(orgFileBROption);
        options.addOption(projectFileOption);
        options.addOption(orgPrjFileOption);
        options.addOption(entityTypeOption);
        options.addOption(DB);
        
        String orgFileAr = null;
        String orgFileBR = null;
        String projectFile = null;
        String bindingFile = null;
        String entityType = null;
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
			
	        if (commandLine.hasOption("orgFileArianna")) {   
	        	orgFileAr = commandLine.getOptionValue("orgFileArianna");
	        	System.out.println("\tArianna organization file: " + orgFileAr);
			} else {
				System.out.println("\tArianna organization file not provided. Use the orgFileArianna option.");
	        	System.exit(1);
			}
	        
	        if (commandLine.hasOption("orgFileBusinessRegistry")) {   
	        	orgFileBR = commandLine.getOptionValue("orgFileBusinessRegistry");
	        	System.out.println("\tBusiness registry organization file: " + orgFileBR);
			} else {
				System.out.println("\tBusiness registry organization file not provided. Use the orgFileBusinessRegistry option.");
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
			
			System.out.println("----------------------------\n");
        
        } catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// END: INPUT PARAMETERS --------------------------------------------------------
        	
        NewAriannaImporter ariannaImporter = new NewAriannaImporter(orgFileAr, orgFileBR, projectFile, bindingFile, header);
        ariannaImporter.importData(entityType, db);

	}
	
	
	public NewAriannaImporter(String orgFile, String orgBRFile, String projectFile, String bindingFile, boolean header) {
		this.orgFile = orgFile;
		this.orgBRFile = orgBRFile;
		this.projectFile = projectFile;
		this.bindingFile = bindingFile;
		// read and load input CSV files
		this.orgReader = initializeCSVReader(orgFile, header, orgSchema);
		this.orgBrReader = initializeCSVReader(orgBRFile, header, orgBrSchema);
		this.projectReader = initializeCSVReader(projectFile, header, projectSchema);
		this.bindingReader = initializeCSVReader(bindingFile, header, bindingSchema);
	}
	
	private CSVReader initializeCSVReader(String inputFile, boolean header, List<String> schema) {
		
		CSVReader reader = null;
		
		// load in memory the CSV file
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
	
	private Organization setOrganizationFields(Map<String,String> data, List<Source> sources) {
		
		// save organization information into Organization object
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
	    	String cleanCity = city.substring(0, 1).toUpperCase() + city.substring(1);
	    	org.setCity(cleanCity);
	    }
	    
	    /*String urbanUnit = data.get("PROVINCIA");
	    if (!urbanUnit.equals("")) {
	    	org.setUrbanUnit(urbanUnit);
	    }*/
	    
	    org.setCountry("Italy");
	    org.setCountryCode("IT");
	    
	    String capSoc = data.get("CAPITALE SOCIALE");
	    if (!capSoc.equals("")) {
	    	if (capSoc.equals("Capitale prevalentemente pubblico")) {
	    		org.setIsPublic("true");
	    	} else {
	    		if (capSoc.equals("Capitale prevalentemente privato italiano")) {
		    		org.setIsPublic("false");
		    	} else { 											// empty field
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
	  	 
		// save organization identifier information into OrganizationIdentifier objects
	  	List<OrganizationIdentifier> orgIds = new ArrayList<OrganizationIdentifier>();
	  	
	  	OrganizationIdentifier orgId1 = null;
	  	String orgIdentifier = data.get("CODICE FISCALE");
	  	if (!orgIdentifier.equals("")) {
	  		orgIdentifier = orgIdentifier.trim();
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
	
	private OrganizationType setOrganizationType(Map<String,String> data, EntityManager em, Map<String, OrganizationType> mapOrgType) {
			
		// save organization type information into OrganizationType object
		OrganizationType orgType = null;
		
		String type = data.get("NATURA GIURIDICA");		
		if(!type.equals("") && type != null) {
			type = getCleanedString(type);
			if (mapOrgType.get(type) != null) {
				orgType = mapOrgType.get(type);
			} else {
				orgType = new OrganizationType();
	        	orgType.setLabel(type);
	        	mapOrgType.put(type, orgType);
			}			
		}
		
		return orgType;
	}
	
	private List<OrganizationExtraField> setOrganizationExtraFields(Map<String,String> data, Set<String> attributes, Organization org) {
		
		// save organization extra information into OrganiationExtraField objects
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
					orgExtraField.setFieldValue(data.get(key).trim());
					orgExtraField.setOrganization(org);
					orgExtraFields.add(orgExtraField);
				}
			}
		}
		
		return orgExtraFields;
	}
	
	private Project setProjectFields(Map<String,String> data) {
		 	 
		// save project information into Project object
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
				e.printStackTrace();
			}
	    }
	    
	    String monthDurationString = data.get("DURATA MESI");
	    if (!monthDurationString.equals("")) {
	    	int monthDuration = Integer.parseInt(monthDurationString); 
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
		
		// save project identifier information into Project identifier object
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
		
		// save project extra information into ProjectExtraField objects
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
	
	public static String getOrgIds(List<OrganizationIdentifier> orgIds) {
		
		// retrieve already inserted organizations
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
	
	private String getCleanedString(String s) {
		// capitalize first letter of a string
		return s.substring(0,1).toUpperCase() + s.substring(1).toLowerCase();
	}
	
	private String getOrgVatCode(List<OrganizationIdentifier> orgIds, String vatCodeString) {			  	
		
		// extract VAT code from list of OrganizationIdentifier objects
		String uniqueOrgId = null;
		for (OrganizationIdentifier orgId : orgIds) {
			if (orgId.getIdentifierName().equals(vatCodeString)) {				
				uniqueOrgId = orgId.getIdentifier();
				uniqueOrgId = uniqueOrgId.replaceFirst("^0+(?!$)", "");
				break;
			}
		}
		
		return uniqueOrgId;
	}
	
	private void mergeOrganizationInformation(EntityManager entitymanager, Organization org, List<Source> sources, List<OrganizationIdentifier> orgIds, Map<String, OrganizationActivity> mapOrgAct, Map<String, OrganizationType> mapOrgType) {
		
		if (org != null) {
								
			String uniqueOrgId = getOrgVatCode(orgIds, "CODICE FISCALE");
			
			// search the organization in business registry via VAT code
			if (uniqueOrgId != null) {
				
				boolean trovato = false;
				if (orgBrData.get(uniqueOrgId) != null) {				// if organization exists in business registry, the organization information already collected are integrated with business registry data
					trovato = true;
					Map<String, String> data = orgBrData.get(uniqueOrgId);
					
					String cityCode = data.get("CCIAA");
					if (!cityCode.equals("")) {
						org.setCityCode(cityCode);
					}
					/*
					String nrea = data.get("NREA");
					if (!nrea.equals("")) {
						org.setCityCode(cityCode);
					}*/
					
					String city = data.get("DESCCCIAA");
					if (!city.equals("")) {
						String cleanCity = city.substring(0, 1).toUpperCase() + city.substring(1);
						org.setCity(cleanCity);
					}										
					
					String label = data.get("DENOMINAZIONE");
					if (!label.equals("")) {
						org.setLabel(label);
					}
					
					/*String urbanUnitCode = data.get("SGLPRVSEDE");
					if (!urbanUnitCode.equals("")) {
						org.setUrbanUnitCode(urbanUnitCode);
					}
					
					String urbanUnit = data.get("DESCPRVSEDE");
					if (!urbanUnit.equals("")) {
						org.setUrbanUnit(urbanUnit);
					}*/
														
					String orgTypeLabel = data.get("DESCNATGIU");				
					if (!orgTypeLabel.equals("")) {
						OrganizationType orgType = null;
						orgTypeLabel = getCleanedString(orgTypeLabel);
						
						if (mapOrgType.containsKey(orgTypeLabel)) {
							orgType = mapOrgType.get(orgTypeLabel);
						} else {
							orgType = new OrganizationType();
							orgType.setLabel(orgTypeLabel);							
							mapOrgType.put(orgTypeLabel, orgType);
						}
	
						org.setOrganizationType(orgType);					
					}
					
					List<OrganizationActivity> activities = new ArrayList<>();
					/*String orgActivityCode = data.get("CODATTIVITAATECO");
					String orgActivityLabel = data.get("DESCATTIVITAATECO");				
					if (!orgActivityCode.equals("") && !orgActivityLabel.equals("")) {
						
						OrganizationActivity orgAct = null;
						
						if (mapOrgAct.containsKey(orgActivityCode)) {
							orgAct = mapOrgAct.get(orgActivityCode);
						} else {
							orgAct = new OrganizationActivity();
							orgAct.setCode(orgActivityCode);
							orgAct.setLabel(orgActivityLabel);
							orgAct.setType("ATECO");
							//entitymanager.persist(orgAct);
							mapOrgAct.put(orgActivityCode, orgAct);
						}															
						activities.add(orgAct);
						org.setActivities(activities);					
					}*/
					
					String orgActivityCode1 = data.get("CODATTIVITA");
					String orgActivityLabel1 = data.get("DESCATTIVITA");
					String orgActivityType1 = data.get("DESCCODIFICA");
					if (!orgActivityCode1.equals("") && !orgActivityLabel1.equals("") && !orgActivityType1.equals("")) {
						
						OrganizationActivity orgAct = null;
						
						if (mapOrgAct.containsKey(orgActivityCode1)) {
							orgAct = mapOrgAct.get(orgActivityCode1);
						} else {
							orgAct = new OrganizationActivity();
							orgAct.setCode(orgActivityCode1);
							orgActivityLabel1 = getCleanedString(orgActivityLabel1);
							orgAct.setLabel(orgActivityLabel1);
							orgAct.setType("Ateco");
							mapOrgAct.put(orgActivityCode1, orgAct);
						}															
						
						/*boolean insert = true;
						for(OrganizationActivity act : activities) {
							if (act.getCode().equals(orgAct.getCode())) {
								insert = false;
								break;
							}
						}
						if (insert) {
							activities.add(orgAct);
							org.setActivities(activities);
						}*/
						activities.add(orgAct);
						org.setActivities(activities);
					}
					
					String address1 = data.get("DESCTOPONSEDE");
					String address2 = data.get("VIASEDE");
					if (!address2.equals("")) {
						String address = null;
						if (!address1.equals("")) {
							address = address1 + " " + address2;
						} else {
							address = address2;
						}
						
						org.setAddress(address);
						org.setAddressSources(sources);
					}
					
					String zipCode = data.get("CAPSEDE");
					if (!zipCode.equals("")) {
						org.setPostcode(zipCode);
					}
					
				}
				
				System.out.println("Org " + uniqueOrgId + ": TROVATO: " + trovato);
			}
			
			entitymanager.persist(org);
		
		}
		
	}
	
	private Map<String,Organization> importOrganizations(EntityManager entitymanager, List<Source> sources) {
		
		this.orgData = createMapFromFileCSV(orgReader, orgSchema);
		
		Map<String, OrganizationType> mapOrgType = new HashMap<>();
		Map<String, OrganizationActivity> mapOrgAct = new HashMap<>();
		
		// retrieve already inserted organizations
    	Query queryType = entitymanager.createQuery("Select t FROM OrganizationType t");        
        List<OrganizationType> types = queryType.getResultList();
        if (types != null && types.size() > 0) {
        	for(OrganizationType t: types) {
        		mapOrgType.put(t.getLabel(), t);
        	}
        } 

		// retrieve already inserted organization activities
    	Query queryActivity = entitymanager.createQuery("Select a FROM OrganizationActivity a");        
        List<OrganizationActivity> acts = queryActivity.getResultList();
        if (acts!=null && acts.size() > 0) {
        	for(OrganizationActivity act: acts) {
        		mapOrgAct.put(act.getCode(), act);
        	}
        }
		
        Map<String, String> vatCodeMap = new HashMap<>(); 
        
		// read organization data and import it into the database 
		System.out.println("Starting importing organizations...");
		entitymanager.getTransaction().begin();
        Map<String,Organization> orgMap = new HashMap<String,Organization>(); 
        Set<String> attributes = new HashSet<String>(Arrays.asList("CODICE FISCALE", "CAR", "DENOMINAZIONE", "INDIRIZZO", "CAP", "COMUNE", "PROVINCIA", "NATURA GIURIDICA"));
        boolean start = true;
        
        // read CSV file line by line
        for(int i=0; i<orgData.size(); i++) {
        	if ((i % 1000) == 0) {
        		System.out.println(i);
        	}
        	
        	Map<String, String> rowData = orgData.get(i);        		
        	
        	String orgVatCode = rowData.get("CODICE FISCALE");
    	  	if (orgVatCode != null && !orgVatCode.equals("")) {
    	  		orgVatCode = orgVatCode.trim();
    	  		orgVatCode = orgVatCode.replaceFirst("^0+(?!$)", "");
    	  		
    	  		if (vatCodeMap.get(orgVatCode) != null) {
        			System.out.println("Duplicated org: " + orgVatCode);
        			continue;
        		} else {
        			vatCodeMap.put(orgVatCode, "");
        		}
    	  	} 
            
        	// set main organization fields
        	Organization org = setOrganizationFields(rowData, sources);
        	
        	// set organization types
        	OrganizationType orgType = setOrganizationType(rowData, entitymanager, mapOrgType);
        	if (orgType != null) {
        		// connect organization type with related organization
        		org.setOrganizationType(orgType);
        	}
        	
        	// set organization identifiers
        	List<OrganizationIdentifier> orgIdentifiers = null;
        	if (org != null) {
        		orgIdentifiers = setOrganizationIdentifiers(rowData, org);

        		// index organization with CAR code
        		String orgId = rowData.get("CAR");
            	if (!orgId.equals("")) {
            		orgMap.put(orgId, org);
            	}
            	
            	// set unique identifier
            	String identifier = sourceName + "::" + getOrgIds(orgIdentifiers);            	
				OrganizationIdentifier localOrgId = new OrganizationIdentifier();
				localOrgId.setIdentifier(identifier);
				localOrgId.setProvenance(sourceName);
				localOrgId.setIdentifierName("lid");
				localOrgId.setVisibility(false);
				localOrgId.setOrganization(org);
				orgIdentifiers.add(localOrgId);
				org.setOrganizationIdentifiers(orgIdentifiers);
        	}        	  	
        	
        	// connect the source to the related organization
        	if (org != null) {        		
        		org.setSources(sources);
        	}

        	// set organization extra fields
        	if (org != null) {
        		List<OrganizationExtraField> orgExtraFields = setOrganizationExtraFields(rowData, attributes, org);
        		org.setOrganizationExtraFields(orgExtraFields);
        	}
        	
        	// merge organization information with data included in business registry
        	mergeOrganizationInformation(entitymanager, org, sources, orgIdentifiers, mapOrgAct, mapOrgType);
        }
        
        entitymanager.getTransaction().commit();
        
        System.out.println("Imported " + orgMap.size() + " organizations");
        
        return orgMap;
	}
	
	private Map<String,Project> importProjects(EntityManager entitymanager, List<Source> sources) {
		
		this.projectData = createMapFromFileCSV(projectReader, projectSchema);
		
		// read project data and import it into the database 
		System.out.println("Starting importing projects...");
		entitymanager.getTransaction().begin();
		Map<String,Project> projectMap = new HashMap<String,Project>(); 
		Set<String> attributes = new HashSet<String>(Arrays.asList("CUR", "TITOLO PROGETTO", "SINTESI PROGETTO", "DATA INIZIO", "DURATA MESI", "COSTI AMMESSI", "PARTECIPANTI"));
		boolean start = true;
		
		// read CSV file line by line
        for(int j=0; j<projectData.size(); j++) {
	        if ((j % 1000) == 0) {
	        		System.out.println(j);
	        	}
        	Map<String, String> rowData = projectData.get(j);        	
        		
        	// set main project fields
        	Project project = setProjectFields(rowData);
        	      	
        	// set project identifiers
        	if (project != null) {
        		List<ProjectIdentifier> projectIdentifiers = setProjectIdentifiers(rowData, project);
        		if (projectIdentifiers != null && projectIdentifiers.size() > 0) {
        			project.setProjectIdentifiers(projectIdentifiers);
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
        		if (projectExtraFields != null && projectExtraFields.size() > 0) {
        			project.setProjectExtraFields(projectExtraFields);
        		}
        	}
        	
        	if (project != null) {
        		entitymanager.persist(project);
        	}      	
        	
        }
        
        entitymanager.getTransaction().commit();
        
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
		
		// retrieve projects
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
        entitymanager.getTransaction().begin();
        int num_bind = 0;
        for (String orgKey : bindingMap.keySet()) {
        	if ((num_bind % 100) == 0) {
        		System.out.println(num_bind);
        	}
            Organization org = orgMap.get(orgKey);
            List<String> projectKeys = bindingMap.get(orgKey);
            List<Project> projects = new ArrayList<Project>();
            if (projectKeys != null) {
	            for(String projectKey: projectKeys) {
	            	Project prj = projectMap.get(projectKey);
	            	if (prj != null) {
		            	projects.add(prj);
	            	}
	            }
	            if (projects.size() > 0) {
	            	if (org != null) {
	            		org.setProjects(projects);
	            		num_bind += projects.size();
	            		entitymanager.merge(org);
	            	}
	            }
	        }
        }
        
        entitymanager.getTransaction().commit();
        System.out.println("Imported " + num_bind + " organization-project links");
	}
	
	
	private Map<String, Map<String, String>> createHashMapFromFileCSV(CSVReader reader, List<String> schema) {
		
		Map<String, Map<String, String>> fullData = new HashMap<>();
		
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
	        	String connectId = rowData.get("CF");
	        	
	        	if (connectId != null && !connectId.equals("")) { 
	        		connectId = connectId.replaceFirst("^0+(?!$)", "");
	        		fullData.put(connectId, rowData);
	        		
	        	}
	        	
	        }
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	    
	    return fullData;
	    
	}
	
	public void importData(String entity, String db) {
				
		// initialize entity manager factory
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
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
    	
    	// read business registry organization file
    	this.orgBrData = createHashMapFromFileCSV(orgBrReader, orgBrSchema);
    	for(String key: orgBrData.keySet()) {
    		Map<String, String> item = orgBrData.get(key);
    		System.out.println("Key: " + key);
    	}  	    	
    	
    	if (entity.equals("all")) {    		
    		Map<String,Organization> orgMap = importOrganizations(entitymanager, sources);
            Map<String,Project> projectMap = importProjects(entitymanager, sources);
            importOrganizationProjectLinks(entitymanager, orgMap, projectMap);			
		}
    	
    	if (entity.equals("organization")) {			
    		Map<String,Organization> orgMap = importOrganizations(entitymanager, sources);
		}
    	
    	if (entity.equals("project")) {			
    		Map<String,Project> projectMap = importProjects(entitymanager, sources);
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
