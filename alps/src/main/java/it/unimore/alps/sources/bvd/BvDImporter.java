package it.unimore.alps.sources.bvd;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.eclipse.persistence.internal.cache.Clearable;

import com.opencsv.CSVReader;

import it.unimore.alps.sources.cercauniversita.CercaUniversitaImporter;
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
import it.unimore.alps.sql.model.Source;

public class BvDImporter {
	
	private CSVReader orgReader;											// organization file reader
	private CSVReader financialReader;										// financial file reader
	private CSVReader leaderReader;											// leader file reader
	List<String> orgSchema = new ArrayList<String>();						// organization file schema
	List<String> financialSchema = new ArrayList<String>();					// financial file schema
	List<String> leaderSchema = new ArrayList<String>();					// leader file schema
	List<Map<String, String>> orgData = new ArrayList<>();					// organization data
	List<Map<String, String>> financialData = new ArrayList<>(); 			// financial data
	List<Map<String, String>> leaderData = new ArrayList<>();				// leader data
	private String sourceName = "Crawled";									// data source name
	private String sourceUrl = "";											// data source url
	private String sourceRevisionDate = "12-02-2018";						// data source date
	
	public static void main(String[] args) {
		
		// BEGIN: INPUT PARAMETERS ------------------------------------------------------
		CommandLine commandLine;
		// organization file
        Option orgFileOption = Option.builder("orgFile")
        		.hasArg()
        		.required(true)
        		.desc("The file that contains organizations data. ")
        		.longOpt("organizationFile")
        		.build();
        // financial file
        Option financialFileOption = Option.builder("financialFile")
        		.hasArg()
        		.required(true)
        		.desc("The file that contains organization financial data.")
        		.longOpt("orgFinancialFile")
        		.build();
        // leader file
        Option leaderFileOption = Option.builder("leaderFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains organization leaders data.")
	            .longOpt("orgLeaderFile")
	            .build();
        // database where to import data
        Option DB = Option.builder("DB").hasArg().required(true).desc("DB. ").longOpt("DB").build();
        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addOption(orgFileOption);
        options.addOption(financialFileOption);
        options.addOption(leaderFileOption);
        options.addOption(DB);
        
        String orgFile = null;
        String financialFile = null;
        String leaderFile = null;
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
			
	        if (commandLine.hasOption("orgFile")) {   
	        	orgFile = commandLine.getOptionValue("orgFile");
	        	System.out.println("\tOrganizations file: " + orgFile);
			} else {
				System.out.println("\tOrganizations file not provided. Use the orgFile option.");
	        	System.exit(1);
			}
			
			if (commandLine.hasOption("financialFile")) {
				financialFile = commandLine.getOptionValue("financialFile");
				System.out.println("\tFinancial file: " + financialFile);
			} else {
				System.out.println("\tFinancial file not provided. Use the financialFile option.");
	        	System.exit(1);
			}
	
			if (commandLine.hasOption("leaderFile")) {
				leaderFile = commandLine.getOptionValue("leaderFile");
				System.out.println("\tLeaders file: " + leaderFile);
			} else {
				System.out.println("\tLeaders file not provided. Use the leaderFile option.");
				System.exit(1);
			}  		
			
			System.out.println("----------------------------\n");
        
        } catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
		// END: INPUT PARAMETERS --------------------------------------------------------
         
        // import data
        System.out.println("Starting importing BvD data...");
        BvDImporter bvdImporter = new BvDImporter(orgFile, financialFile, leaderFile, header);
        bvdImporter.importData(db);

	}
	
	public BvDImporter(String orgFile, String financialFile, String leaderFile, boolean header) {
		// read and load in memory the input CSV files 
		this.orgReader = initializeCSVReader(orgFile, header, orgSchema);
		this.financialReader = initializeCSVReader(financialFile, header, financialSchema);
		this.leaderReader = initializeCSVReader(leaderFile, header, leaderSchema);
		
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
						for(int i=0; i<line.length; i++) {
							String attrName = line[i];
							if (attrName.equals("")) {
								attrName = "Id";
							}
							schema.add(attrName);	
						}
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
	
	private List<PersonIdentifier> setPersonIdentifiers(Map<String,String> data, Person person) {
	  	 
		// save person identifier information into PersonIdentifier object
	  	List<PersonIdentifier> pIds = new ArrayList<PersonIdentifier>();
	  	
	  	PersonIdentifier pId = null;
	  	String pIdentifier = data.get("id");
	  	if (!pIdentifier.equals("")) {
	  		pId = new PersonIdentifier();
	  		pId.setIdentifier(pIdentifier);
	  		pId.setProvenance(sourceName); 		
	  		pId.setIdentifierName("id");
	  		pId.setPerson(person);
	  		pIds.add(pId);
	  	}	  
	  	
	  	return pIds;
	  	 
	}
	
	private Map<String,List<Leader>> importLeaders(EntityManager entitymanager, List<Source> sources) {
		
		// read leader data 
		System.out.println("Starting extracting leader data...");
		entitymanager.getTransaction().begin();
        Map<String,List<Leader>> orgLeadersMap = new HashMap<String,List<Leader>>();
        Map<String,Date> leaderDateMap = new HashMap<String,Date>();

        // read CSV file line by line
        for(int i=0; i<leaderData.size(); i++) {
        	if ((i % 1000) == 0) {
        		System.out.println(i);
        	}
        	
        	// CSV data row
        	Map<String, String> rowData = leaderData.get(i);
        	
        	Leader leader = new Leader();
        	boolean isLeader = false;
        	
        	String title = rowData.get("positions"); 
            if (!title.equals("")) {
            	leader.setTitle(title);
            	
            	if (title.equals("President / Chairman") || title.equals("Senior management employee")) {
            		isLeader = true;
            	}
            }
            
            if (isLeader) {           
	            
	            String bvdId = rowData.get("bvdId");
	            String leaderId = rowData.get("id");                        
	            
	            String nominationDateString = rowData.get("nominationDate");
	            Date nominationDate = null;
	            if (!nominationDateString.equals("")) {
		            SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");
		    		
		            try {
		            	nominationDate = formatter.parse(nominationDateString);
		
		            } catch (ParseException e) {
		                e.printStackTrace();
		            }
	            }
	            
	            String firstName = rowData.get("firstname"); 
	            if (!firstName.equals("")) {
	            	leader.setFirstName(firstName);
	            }                       
	            
	            String lastName = rowData.get("lastname"); 
	            if (!lastName.equals("")) {
	            	leader.setLastName(lastName);
	            }
	            
	            leader.setSources(sources);	            
	            
	            String type = rowData.get("type");
	            // only "individual" data are inserted (not "company type")
	            if (type.equals("Individual")) {
	            	            	
	            	if (orgLeadersMap.get(bvdId) == null) { 		// no leader for this organization has been found previously
	            		List<Leader> leaders = new ArrayList<Leader>();
	            		leaders.add(leader);
	            		orgLeadersMap.put(bvdId, leaders);
	            		
	            		// storing the leader id with the most recent role             		            		
	                    if(nominationDate != null) {            		
	                    	leaderDateMap.put(leaderId, nominationDate);
	                    }
	                    
	            	} else {										// a leader for this organization has been already found
	            		
	            		if (leaderDateMap.get(leaderId) != null) {	// a leader identified by leaderId has been already found
	            			if (nominationDate != null) {
		            			if (nominationDate.after(leaderDateMap.get(leaderId))) {	// if there is a most recent role for the current leader 
		            				// leader removal from list
		            				List<Leader> oldLeaders = orgLeadersMap.get(bvdId);
		            				
		            				for (Iterator<Leader> iter = oldLeaders.listIterator(); iter.hasNext(); ) {
		            					Leader ol = iter.next();
		            				    if (ol.getFirstName().equals(leader.getFirstName()) && ol.getLastName().equals(leader.getLastName())) {
		            				        iter.remove();
		            				        break;
		            				    }
		            				}
		            				            				
		            				orgLeadersMap.get(bvdId).add(leader);
		                    		leaderDateMap.put(leaderId, nominationDate);
		            			}
	            			} else {
	            				List<Leader> oldLeaders = orgLeadersMap.get(bvdId);
	            				boolean insertLeader = true;
	            				for(Leader ll: oldLeaders) {
	            					if (ll.getFirstName().equals(leader.getFirstName()) && ll.getLastName().equals(leader.getLastName())) {
	            						insertLeader = false;
	            						break;
	            					}
	            				}
	            				if (insertLeader == true) {
	            					orgLeadersMap.get(bvdId).add(leader);
	            				}
	            			}
	            		} else {															// no leader identified by leaderId has been found
	            			orgLeadersMap.get(bvdId).add(leader);
	                		leaderDateMap.put(leaderId, nominationDate);
	            		}
	            		
	            	}
	            }
	            
	        	entitymanager.persist(leader);        	
            }
        }
        
        entitymanager.getTransaction().commit();
        System.out.println("Extracted leader data.");
        
        return orgLeadersMap;
	}
	
	private Map<String,List<Person>> importPeople(EntityManager entitymanager, List<Source> sources) {
		
		// read person data 
		System.out.println("Starting extracting leader data...");
		entitymanager.getTransaction().begin();
        Map<String,List<Person>> orgLeadersMap = new HashMap<String,List<Person>>();
        Map<String,Date> leaderDateMap = new HashMap<String,Date>();

        // read CSV file line by line
        for(int i=0; i<leaderData.size(); i++) {
        	if ((i % 1000) == 0) {
        		System.out.println(i);
        	}
        	
        	// CSV data row
        	Map<String, String> rowData = leaderData.get(i);
        	
        	Person leader = new Person();
        	boolean isNotLeader = true;
        	
        	String title = rowData.get("positions"); 
            if (!title.equals("")) {
            	leader.setTitle(title);
            	
            	if (title.equals("President / Chairman") || title.equals("Senior management employee")) {
            		isNotLeader = false;
            	}
            }
            
            if (isNotLeader) {            
	            
	            String bvdId = rowData.get("bvdId");
	            String leaderId = rowData.get("id");                        
	            
	            String nominationDateString = rowData.get("nominationDate");
	            Date nominationDate = null;
	            if (!nominationDateString.equals("")) {
		            SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");
		    		
		            try {
		            	nominationDate = formatter.parse(nominationDateString);
		
		            } catch (ParseException e) {
		                e.printStackTrace();
		            }
	            }
	            
	            String firstName = rowData.get("firstname"); 
	            if (!firstName.equals("")) {
	            	leader.setFirstName(firstName);
	            }
	            
	            String lastName = rowData.get("lastname"); 
	            if (!lastName.equals("")) {
	            	leader.setLastName(lastName);
	            }
	            
	            leader.setSources(sources);
	            
	            // set person identifiers
	        	if (leader != null) {
	        		List<PersonIdentifier> personIdentifiers = setPersonIdentifiers(rowData, leader);
	        		// create PersonIdentifier tuples in the DB
	        		for (PersonIdentifier personIdentifier: personIdentifiers) {
	        			entitymanager.persist(personIdentifier);
	        		}
	        	}
	            
	            String type = rowData.get("type");
	            // only "individual" data are inserted (not "company type")
	            if (type.equals("Individual")) {
	            	            	
	            	if (orgLeadersMap.get(bvdId) == null) { 		// no person for this organization has been found previously
	            		List<Person> leaders = new ArrayList<Person>();
	            		leaders.add(leader);
	            		orgLeadersMap.put(bvdId, leaders);
	            		
	            		// storing the person id with the most recent role            		            		
	                    if(nominationDate != null) {            		
	                    	leaderDateMap.put(leaderId, nominationDate);
	                    }
	                    
	            	} else {										// a person working in this organization has been already found
	            		
	            		if (leaderDateMap.get(leaderId) != null) {	// a person identified by leaderId has been already found
	            			if (nominationDate != null) {
		            			if (nominationDate.after(leaderDateMap.get(leaderId))) {	// if there is a most recent role for the current leader
		            				// leader removal from list
		            				List<Person> oldLeaders = orgLeadersMap.get(bvdId);
		            				
		            				for (Iterator<Person> iter = oldLeaders.listIterator(); iter.hasNext(); ) {
		            				    Person ol = iter.next();
		            				    if (ol.getFirstName().equals(leader.getFirstName()) && ol.getLastName().equals(leader.getLastName())) {
		            				        iter.remove();
		            				        break;
		            				    }
		            				}
		            				            				
		            				orgLeadersMap.get(bvdId).add(leader);
		                    		leaderDateMap.put(leaderId, nominationDate);
		            			}
	            			} else {
	            				List<Person> oldLeaders = orgLeadersMap.get(bvdId);
	            				boolean insertLeader = true;
	            				for(Person ll: oldLeaders) {
	            					if (ll.getFirstName().equals(leader.getFirstName()) && ll.getLastName().equals(leader.getLastName())) {
	            						insertLeader = false;
	            						break;
	            					}
	            				}
	            				if (insertLeader == true) {
	            					orgLeadersMap.get(bvdId).add(leader);
	            				}
	            			}
	            		} else {									// no leader identified by leaderId has been found
	            			orgLeadersMap.get(bvdId).add(leader);
	                		leaderDateMap.put(leaderId, nominationDate);
	            		}
	            		
	            	}
	            }
	            
	        	entitymanager.persist(leader);        	
            }
        }
        
        entitymanager.getTransaction().commit();
        System.out.println("Extracted leader data.");
        
        return orgLeadersMap;
	}
	
	private Map<String,Organization> importFinancialData() {
		
		// read financial data 
		System.out.println("Starting extracting financial data...");
        Map<String,Organization> orgFinancialDataMap = new HashMap<String,Organization>();
        
        // read CSV file line by line
        for(int i=0; i<financialData.size(); i++) {
        	if ((i % 1000) == 0) {
        		System.out.println(i);
        	}
        	
        	// CSV data row
        	Map<String, String> rowData = financialData.get(i);                            
            
        	Organization org = new Organization();
            String bvdId = rowData.get("bvdId");
            
            Date closeDate = null;
            String closeDateString = rowData.get("closeDate");
            if(!closeDateString.equals("")) {
            	SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");
    		
	            try {
	            	closeDate = formatter.parse(closeDateString);
	
	            } catch (ParseException e) {
	                
	                SimpleDateFormat bisFormatter = new SimpleDateFormat("yyyy");
	                try {
		            	closeDate = bisFormatter.parse(closeDateString);
		
		            } catch (ParseException e1) {
		            	System.out.println("Date missing.");
		            }
	            }      
            }
            
            String employees = rowData.get("employees"); 
            if (!employees.equals("")) {
            	org.setFinancePrivateEmployees(employees);
            }
            
            String revenue = rowData.get("Operational Revenu"); 
            if (!revenue.equals("")) {
            	org.setFinancePrivateRevenueRange(revenue);
            }
            
            if (closeDate != null) {
            	org.setFinancePrivateDate(closeDate);
            }
            
            if (orgFinancialDataMap.get(bvdId) != null) { 	// extracted financial data belonging to an organization already processed
            	Date oldDate = orgFinancialDataMap.get(bvdId).getFinancePrivateDate();
            	if (closeDate != null) {
	            	// if current financial data is more recent than the previous one, update data
	            	if (closeDate.after(oldDate)) {
	            		orgFinancialDataMap.put(bvdId, org);
	            	}
            	}
            } else {										// new organization financial data
            	orgFinancialDataMap.put(bvdId, org);
            }                    	
        }
        
        System.out.println("Extracted financial data.");
        
        return orgFinancialDataMap;
	}
	
	private Organization setOrganizationFields(Map<String,String> data, Map<String,Organization> orgFinancialMap, List<Source> sources) {
				
		
		Organization org = new Organization();
		
    	String addr = data.get("addr");
		if (!addr.equals("") && !addr.equals("Credit needed")) {
			org.setAddressSources(sources);
			org.setAddress(addr);
		}
	    
	    String city = data.get("city");
	    if (!city.equals("")) {
	    	String cleanCity = getCleanedString(city);
	    	org.setCity(cleanCity);
	    }
	    
	    org.setCountryCode("IT");
    	org.setCountry("Italy");
	    
	    String creationDateString = data.get("Creation date");
	    Date creationDate = null;
        if (!creationDateString.equals("")) {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");
    		
            try {
            	creationDate = formatter.parse(creationDateString);

            } catch (ParseException e) {
            	SimpleDateFormat bisFormatter = new SimpleDateFormat("yyyy");
                try {
                	creationDate = bisFormatter.parse(creationDateString);
	
	            } catch (ParseException e1) {
	            	System.out.println("Date missing.");
	            }
            }
        }
        if (creationDate != null) {
        	org.setCreationYear(creationDate);
        }
	    	        
	    String legalForm = data.get("Legal Form");
	    if (!legalForm.equals("")) {
	    	if (legalForm.equals("Public")) {
	    		org.setIsPublic("true");
	    	} else {
	    		if (legalForm.equals("Private")) {
	    			org.setIsPublic("false");
	    		}
	    	}
	    }
	    
	    String label = data.get("name");
	    if (!label.equals("")) {
	    	org.setLabel(label);
	    }
	    
	    String postcode = data.get("postcode");
	    if (!postcode.equals("")) {
	    	org.setPostcode(postcode);
	    }
	    
	    String bvdId = data.get("bvdId");
	    if(!bvdId.equals("")) {
	    	Organization orgFinancialData = orgFinancialMap.get(bvdId);
	    	if (orgFinancialData != null) {
	    		org.setFinancePrivateEmployees(orgFinancialData.getFinancePrivateEmployees());
	    		org.setFinancePrivateRevenueRange(orgFinancialData.getFinancePrivateRevenueRange());
	    		org.setFinancePrivateDate(orgFinancialData.getFinancePrivateDate());
	    	}
	    }
	    
	    return org;
	    	
	}
	
	private String getCleanedString(String s) {
		// capitalize first letter of the string
		return s.substring(0,1).toUpperCase() + s.substring(1).toLowerCase();
	}
	
	private OrganizationType setOrganizationType(Map<String,String> data, EntityManager em, Map<String, OrganizationType> mapOrgType) {
		
		OrganizationType orgType = null;
		
		String type = data.get("Legal Form (detailed)");
		if(!type.equals("") && !type.equals("Credit needed")) {
			
			type = getCleanedString(type);
			
	        if (mapOrgType.get(type)!=null) {
	        	orgType = mapOrgType.get(type); 
	        } else {
	        	orgType = new OrganizationType();
	        	orgType.setLabel(type);
	        	em.persist(orgType);
	        	mapOrgType.put(type, orgType);
	        }

		}
		
		
		return orgType;
	}
	
	private List<OrganizationIdentifier> setOrganizationIdentifiers(Map<String,String> data, Organization org) {
	  	 
		// save organization identifier information into OrganizationIdentifier object
	  	List<OrganizationIdentifier> orgIds = new ArrayList<OrganizationIdentifier>();	  	
	  	
	  	OrganizationIdentifier orgId = null;
	  	String orgIdentifier = data.get("bvdId");
	  	if (!orgIdentifier.equals("")) {
	  		orgId = new OrganizationIdentifier();
	  		orgId.setIdentifier(orgIdentifier);
	  		orgId.setProvenance(sourceName); 		
	  		orgId.setIdentifierName("webId");
	  		orgId.setOrganization(org);
	  		orgId.setVisibility(false);
	  		orgIds.add(orgId);
	  	}	  	
	  	
	  	return orgIds;
	  	 
	}
	
	private Link setOrganizationLinks(Map<String,String> data) {
		
		// save organization website information into Link object
		Link link = null;
		
		if (!data.get("website").equals("") && data.get("website") != null) {
			link = new Link();
	    	link.setUrl(data.get("website"));
	    	link.setType("main");
	    	link.setLabel("homepage");
	    }
	    
	    return link;
	}
	
	private List<OrganizationActivity> setOrganizationActivities(EntityManager em, Map<String,String> data, Map<String, OrganizationActivity> mapOrgAct) {
		
		// save organization activity information into OrganizationActivity objects
		List<OrganizationActivity> orgActivities = new ArrayList<OrganizationActivity>();		
		
		OrganizationActivity orgActivity = null;
		String code = data.get("NACE rev2");
		String label = data.get("NACE rev2 description");
		
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
		
		if (!code.equals("") && !label.equals("") && !code.equals("Credit needed") && isNumber) {
			
			if (mapOrgAct.get(code)!=null) {
				orgActivity = mapOrgAct.get(code);
			} else {
				orgActivity = new OrganizationActivity();
	    		orgActivity.setCode(code);
	    		label = getCleanedString(label);
	    		orgActivity.setLabel(label);
	    		orgActivity.setType("Ateco");
	    		em.persist(orgActivity);
	    		mapOrgAct.put(code, orgActivity);	    		
			}
			orgActivities.add(orgActivity);
		}
		
		
		return orgActivities;
	}	
	
	public static String getOrgIds(List<OrganizationIdentifier> orgIds) {
		
		// retrieve organizations alredy inserted
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
	
	private void importOrganizations(EntityManager entitymanager, List<Source> sources, Map<String,List<Person>> orgPeopleMap, Map<String,List<Leader>> orgLeadersMap, Map<String,Organization> orgFinancialMap) {

		// read organization data and import it into the database 		
		Map<String, OrganizationType> mapOrgType = new HashMap<>();
		Map<String, OrganizationActivity> mapOrgAct = new HashMap<>();
		
    	Query queryType = entitymanager.createQuery("Select t FROM OrganizationType t");        
        List<OrganizationType> types = queryType.getResultList();
        if (types != null && types.size() > 0) {
        	for(OrganizationType t: types) {
        		mapOrgType.put(t.getLabel(), t);
        	}
        } 
    	
    	Query queryActivity = entitymanager.createQuery("Select a FROM OrganizationActivity a");        
        List<OrganizationActivity> acts = queryActivity.getResultList();
        if (acts!=null && acts.size() > 0) {
        	for(OrganizationActivity act: acts) {
        		mapOrgAct.put(act.getCode(), act);
        	}
        }
		
		
		System.out.println("Starting importing organizations...");
		entitymanager.getTransaction().begin(); 
        Set<String> attributes = new HashSet<String>(Arrays.asList("NACE rev2","NACE rev2 description","addr","bvdId","city","ctryIso","Creation date","Legal Form","name","postcode","website"));        
        int i;
        
        // read CSV file line by line
        for(i=0; i<orgData.size(); i++) {
        	if ((i % 1000) == 0) {
        		System.out.println(i);
        	}
        	
        	// CSV data row
        	Map<String, String> rowData = orgData.get(i);        		                                                                 
            
        	// set main organization fields
        	Organization org = setOrganizationFields(rowData, orgFinancialMap, sources);
        	
        	// set organization types
        	OrganizationType orgType = setOrganizationType(rowData, entitymanager, mapOrgType);
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
        		String identifier = sourceName + "::" + org.getLabel() +"::" + getOrgIds(orgIdentifiers);
				OrganizationIdentifier orgId = new OrganizationIdentifier();
				orgId.setIdentifier(identifier);
				orgId.setProvenance(sourceName);
				orgId.setIdentifierName("lid");
				orgId.setVisibility(false);
				orgId.setOrganization(org);
				entitymanager.persist(orgId);
        	}
        	
        	// leaders
        	String orgId = rowData.get("bvdId");
        	if (!orgId.equals("")) {
        		List<Person> people = orgPeopleMap.get(orgId);
        		if (people != null) {
        			org.setPeople(people);
        		}
        		List<Leader> leaders = orgLeadersMap.get(orgId);
        		if (leaders != null) {
        			org.setLeaders(leaders);
        		}
        	}
        	
        	// connect the source to the related organization
        	if (org != null) {        		
        		org.setSources(sources);
        	}
        	
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
        	
        	// organization activities
        	List<OrganizationActivity> activities = setOrganizationActivities(entitymanager, rowData, mapOrgAct);
        	// connect the activities to the related organization
        	if (org != null) {
        		org.setActivities(activities);
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
        	
        }
        
        entitymanager.getTransaction().commit();
        System.out.println("Imported " + i + " organizations");
        
	}
	
	private List<OrganizationExtraField> setOrganizationExtraFields(Map<String,String> data, Set<String> attributes, Organization org) {
		
		// save organization extra file information into OrganizationExtraField objects
		List<OrganizationExtraField> orgExtraFields = new ArrayList<OrganizationExtraField>();
		
		Set<String> visibleAttributes = new HashSet<String>(Arrays.asList("akaName", "Company size", "email", "fax", "Legal Form (detailed)","natIdLabel","natIdNumber","opGDesc","opGName","phone","Standardized Legal Form","status","Trade Description"));
		
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
	
	public void importData(String db) {
	
		// initialize entity manager factory
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();		
		
		this.orgData = createMapFromFileCSV(orgReader, orgSchema);
        this.financialData = createMapFromFileCSV(financialReader, financialSchema);
        this.leaderData = createMapFromFileCSV(leaderReader, leaderSchema);        
    	
        // get or create bvd source ---------------------------------------------------------------
    	entitymanager.getTransaction().begin();
        Query query = entitymanager.createQuery("Select s FROM Source s where s.label = :label");
        query.setParameter("label", sourceName);
        List<Source> bvdSource = query.getResultList();
        Source source = null;
        if (bvdSource.size() > 0) {
        	source = bvdSource.get(0);
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
    	
    	Map<String,List<Person>> orgPeopleMap = importPeople(entitymanager, sources);
    	Map<String,List<Leader>> orgLeadersMap = importLeaders(entitymanager, sources);
    	Map<String,Organization> orgFinancialMap = importFinancialData();
    	importOrganizations(entitymanager, sources, orgPeopleMap, orgLeadersMap, orgFinancialMap);    	    	
    	
		entitymanager.close();
		emfactory.close();
	}

}
