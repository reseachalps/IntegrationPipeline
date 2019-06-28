package it.unimore.alps.sources.fwf;

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
import java.util.GregorianCalendar;
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

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

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
import it.unimore.alps.sql.model.Thematic;

public class FwfImporter {
	
	private CSVReader projectReader;														// project file reader
	List<String> projectSchema = new ArrayList<String>();									// project file schema
	List<Map<String, String>> projectData = new ArrayList<>();								// project data
	private String sourceName = "Austrian Science Fund (FWF)";								// data source name
	private String sourceUrl = "https://www.fwf.ac.at/en/";									// data source url
	private String sourceRevisionDate = "01-08-2018";										// data source date
	// list of valid organizations to be imported
	private Set<String> exactMatchLabels = new HashSet<String>(Arrays.asList("Universität Wien", "Universität Graz", "Universität Innsbruck", "Paris-Lodron-Universität Salzburg",
			"Technische Universität Wien", "Technische Universität Graz", "Veterinärmedizinische Universität Wien",		
			"Montanuniversität Leoben", "Wirtschaftsuniversität Wien", "Universität für Bodenkultur Wien", 
			"Universität Linz", "Universität Klagenfurt", "Akademie der bildenden Künste Wien", "Universität für angewandte Kunst Wien",
			"Universität für Musik und darstellende Kunst Wien", "Universität Mozarteum Salzburg", "Universität für Musik und darstellende Kunst Graz",
			"Universität für künstlerische und industrielle Gestaltung Linz", "sonstige außeruniv. Forschungsstätte",
			"Österreichische Akademie der Wissenschaften", "ausländische Forschungsstätte", "Medizinische Universität Wien",
			"Medizinische Universität Graz", "Medizinische Universität Innsbruck", "Donau-Universität Krems",
			"Institute of Science and Technology Austria", "Gregor Mendel Institut für Molekulare Pflanzenbiologie",
			"Institut für Molekulare Biotechnologie", "CeMM- Center for Molecular Medicine")); 		
	private Set<String> subLabels = new HashSet<String>(Arrays.asList("Wien", "Graz", "Innsbruck", "Salzburg", "Leoben", "Linz", "Klagenfurt", "Krems", "Austria", "Österreichische", "Austrian"));
	
	
	
	public static void main(String[] args) {
		
		// BEGIN: INPUT PARAMETERS ------------------------------------------------------
		CommandLine commandLine;
		// project file
        Option projectFileOptions = Option.builder("projectFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains fwf projects. ")
	            .longOpt("projectFile")
	            .build();
        // database where to import data
        Option DB = Option.builder("DB").hasArg().required(true).desc("DB. ").longOpt("DB").build();

        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addOption(projectFileOptions);
        options.addOption(DB);
        
        String projectFile = null;
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
			
	        if (commandLine.hasOption("projectFile")) {   
	        	projectFile = commandLine.getOptionValue("projectFile");
	        	System.out.println("\tFwf project file: " + projectFile);
			} else {
				System.out.println("\tFwf project file not provided. Use the projectFile option.");
	        	System.exit(1);
			}  								
			
			System.out.println("----------------------------\n");
        
        } catch (org.apache.commons.cli.ParseException e) {
			e.printStackTrace();
		}        
		// END: INPUT PARAMETERS --------------------------------------------------------
		
        // import data
        FwfImporter fwfImporter = new FwfImporter(projectFile, header);
        fwfImporter.importData(db);

	}
	
	public FwfImporter(String projectFile, boolean header) {
		// load CSV file data in memory
		this.projectReader = initializeCSVReader(projectFile, header, projectSchema);		
		this.projectData = createMapFromFileCSV(projectReader, projectSchema);
	}
	
	private CSVReader initializeCSVReader(String inputFile, boolean header, List<String> schema) {
		
		CSVReader reader = null;
		
		// load CSV file in memory
		CSVParser parser = new CSVParserBuilder().withSeparator('|').withIgnoreQuotations(true).build();
		try {
			reader = new CSVReaderBuilder(new FileReader(inputFile)).withCSVParser(parser).build();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
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
	
	private boolean areOrganizationsValid(List<String> singleOrgs) {
		
		// filter out organizations that don't appertain to Alpine region
		boolean valid = false; 
		
		if (singleOrgs != null) {
			for(String singleOrg: singleOrgs) { // if only one organization respects the filter condition, all organizations are to be considered valid

				if (singleOrg != null && !singleOrg.equals(" ") && !singleOrg.equals("")) {
					singleOrg = singleOrg.trim().toLowerCase();
				
					// if the organization is included in exactMatchLabels, it is a valid organization 
					for(String str: this.exactMatchLabels) {
					    if(str.trim().toLowerCase().contains(singleOrg)) {
					    	valid = true;
					    	break;
					    }					    	
					}

					// if the organization contains a label included in subLabels, it is a valid organization
					for(String str: this.subLabels) {
					    if(singleOrg.contains(str.trim().toLowerCase())) {
					    	valid = true;
					    	break;
					    }					    	
					}
					
				} else {
					break;
				}
			}
		}
		
		return valid;
		
	}
	
	private List<Organization> setParentChildOrganizations(EntityManager entitymanager, Map<String,String> data, List<Source> sources, Map<String,Organization> orgMap) {
		
		List<Organization> orgs = new ArrayList<>();
		
    	String orglabels = data.get("research place & institute");
		if (orglabels != null && !orglabels.equals("")) {
			List<String> coupleOrgs = Arrays.asList(orglabels.split(";"));
			if (coupleOrgs!=null) {
				for (String coupleOrg: coupleOrgs) {					
					List<String> singleOrgs = Arrays.asList(coupleOrg.split(" - "));
					
					if (areOrganizationsValid(singleOrgs)) {
					
						if (singleOrgs != null) {
							
							Organization parentOrg = null;
							Organization childOrg = null;
							boolean newParentOrg = false;
							boolean newChildOrg = false;
							for (int i=0; i<singleOrgs.size(); i++) {
								String singleOrg = singleOrgs.get(i);
								singleOrg = singleOrg.trim();
								
								if (singleOrg != null && !singleOrg.equals(" ") && !singleOrg.equals("")) {
									if (i == 0) { // parent												
										
										if (!orgMap.containsKey(singleOrg)) {
											parentOrg = new Organization();
											parentOrg.setLabel(singleOrg);
											parentOrg.setCountry("Austria");
											parentOrg.setCountryCode("AT");
											parentOrg.setSources(sources);
											parentOrg.setIsPublic("true");
											newParentOrg = true;
										} else {
											parentOrg = orgMap.get(singleOrg);
										}
									} else { // child										
										
										if (!singleOrg.equals(parentOrg.getLabel())) {
										
											if (!orgMap.containsKey(coupleOrg)) {
												childOrg = new Organization();
												childOrg.setLabel(singleOrg);
												childOrg.setCountry("Austria");
												childOrg.setCountryCode("AT");
												childOrg.setSources(sources);
												childOrg.setIsPublic("true");
												newChildOrg = true;
											} else {
												childOrg = orgMap.get(coupleOrg);
											}
										}
									}
								}														
								
								if (parentOrg != null && childOrg != null) { // connect parent org with child org
									List<Organization> childrenOrgs = parentOrg.getChildrenOrganizations(); 
									
									if (childrenOrgs!=null) { // parent org has already some children organizations
										
										boolean insert = true;
										Organization matchedOrg = null;
										for (Organization org: childrenOrgs) { // check for duplicate children organizations
											if (childOrg.getLabel().equals(org.getLabel())) {
												insert = false;
												matchedOrg = org;
												orgs.add(matchedOrg);
												break;
											}
										}
										if (insert) {
											childrenOrgs.add(childOrg);										
											parentOrg.setChildrenOrganizations(childrenOrgs);										
											
											if (newParentOrg) {
												entitymanager.persist(parentOrg);
												orgMap.put(parentOrg.getLabel(), parentOrg);
											} else {
												entitymanager.merge(parentOrg);
											}
											
											if (newChildOrg) {
												entitymanager.persist(childOrg);
												orgMap.put(coupleOrg, childOrg);
											}
											
											orgs.add(childOrg);
										}
									} else { // parent org has no children organizations yet
										childrenOrgs = new ArrayList<>();
										childrenOrgs.add(childOrg);									
										parentOrg.setChildrenOrganizations(childrenOrgs);
										
										if (newParentOrg) {
											entitymanager.persist(parentOrg);
											orgMap.put(parentOrg.getLabel(), parentOrg);
										} else {
											entitymanager.merge(parentOrg);
										}
										
										if (newChildOrg) {
											entitymanager.persist(childOrg);
											orgMap.put(childOrg.getLabel(), childOrg);
										}
										
										orgs.add(childOrg);
									}
								} else { // save parent and child orgs without connect them
									
									if (parentOrg != null) {
										if (newParentOrg) {
											entitymanager.persist(parentOrg);
											orgMap.put(parentOrg.getLabel(), parentOrg);
										}
										orgs.add(parentOrg);
									}
									
									if (childOrg != null) {
										if (newChildOrg) {
											entitymanager.persist(childOrg);
											orgMap.put(childOrg.getLabel(), childOrg);
										}
										orgs.add(childOrg);
									}
								}
							}
						}
						
					}
				}
			}
			

		}
	    
	    return orgs;
		
	}	
	
	private List<ProjectExtraField> setProjectExtraFields(Map<String,String> data, Set<String> attributes, Project prj) {
		
		// save project extra information into ProjectExtraField
		List<ProjectExtraField> prjExtraFields = new ArrayList<ProjectExtraField>();				
		
		for (String key : data.keySet()) {
			if (attributes.contains(key)) {
				if (!data.get(key).equals("")) {
					ProjectExtraField prjExtraField = new ProjectExtraField();
					prjExtraField.setVisibility(true);
					prjExtraField.setFieldKey(key);
					prjExtraField.setFieldValue(data.get(key));
					prjExtraField.setProject(prj);
					prjExtraFields.add(prjExtraField);
				}
			}
		}
		
		return prjExtraFields;
	}
	
	private Project setProject(Map<String,String> data, List<Source> sources) {
		
		// save project information into Project object
		Project prj = null;
		
		String title = data.get("project title");
		if (!title.equals("")) {
	    	prj = new Project();
	    	prj.setLabel(title);
		
			String url = data.get("website project");
			if (url!= null && !url.equals("")) {
		    	prj.setUrl(url);
		    }
			
			Date startDate = null;
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			String start = data.get("from");
			if (start != null && !start.equals("")) {		    								
					
				try {
					startDate = df.parse(start);
					prj.setStartDate(startDate);
				} catch (java.text.ParseException e) {
					e.printStackTrace();
				}				
			}
			
			if (startDate!=null) {
				Calendar startCalendar = new GregorianCalendar();
				startCalendar.setTime(startDate);
				int month = startCalendar.get(Calendar.MONTH)+1;
				int year = startCalendar.get(Calendar.YEAR);
				prj.setMonth(""+month);
				prj.setYear(""+year);
				
				String end = data.get("till");
				if (end != null && !end.equals("")) {

					Date endDate;
					try {
						endDate = df.parse(end);						
						Calendar endCalendar = new GregorianCalendar();
						endCalendar.setTime(endDate);
						int diffYear = endCalendar.get(Calendar.YEAR) - startCalendar.get(Calendar.YEAR);
						int diffMonth = diffYear * 12 + endCalendar.get(Calendar.MONTH) - startCalendar.get(Calendar.MONTH);
						prj.setDuration(""+diffMonth);
						
					} catch (java.text.ParseException e) {
						e.printStackTrace();
					}
				}
				
				
			}
			
			String budget = data.get("grants awarded");
			if (budget != null && !budget.equals("")) {
		    	prj.setBudget(budget);
		    }
			
			prj.setSources(sources);

		}
		
		return prj;
		
	}
	
	private List<Thematic> setProjectThematics(EntityManager entitymanager, Map<String,String> data, Map<String, Thematic> prjThematicsMap) {
		
		// save project thematic information into Thematic objects
		List<Thematic> thematics = new ArrayList<>();
		
		String themes = data.get("science discipline");
		if (themes != null && !themes.equals("")) {
			themes = themes.replaceAll("\\d+\\.\\d+%","");
			List<String> themesList = Arrays.asList(themes.split(","));
			if (themesList != null && themesList.size() > 0) {
				for (String theme: themesList) {
					theme = theme.trim();
					if (!theme.equals("") && !theme.equals(" ")) {
						Thematic thematic = null;
						if (prjThematicsMap.containsKey(theme)) {
							thematic = prjThematicsMap.get(theme);
						} else {
							thematic = new Thematic();
							thematic.setLabel(theme);
							prjThematicsMap.put(theme, thematic);
						}
						
						boolean insert = true;
						for (Thematic th: thematics) {						
							if (th.getLabel().equals(thematic.getLabel())) {
								insert = false;
								break;
							}						
						}
						if (insert) {
							thematics.add(thematic);
						}
					}
				}
			}
	    }
		
		return thematics;
		
	}			
	
	private Map<String, Project> importProjects(List<Map<String, String>> data, EntityManager entitymanager, List<Source> sources) {
		
		System.out.println("Starting importing projects...");
		
		Map<String, Project> projectMap = new HashMap<>();				
		Map<String, Thematic> prjThematicsMap = new HashMap<>();
				         
		// read CSV file line by line
        for(int i=0; i<data.size(); i++) {
        	if ((i % 1000) == 0) {
        		System.out.println(i);
        	}
        	
        	// CSV data row
        	Map<String, String> rowData = data.get(i);
        	
        	// set project fields
        	Project prj = setProject(rowData, sources);
        	       	
        	if (prj!=null) {
        		      	        	
	        	// project id	        	
	        	String prjNumber = rowData.get("project number");
				if (prjNumber != null && !prjNumber.equals("")) {
					
					List<ProjectIdentifier> prjIds = new ArrayList<>();
					ProjectIdentifier prjId = new ProjectIdentifier();
					prjId.setIdentifier(prjNumber);
					prjId.setIdentifierName("Project number");
					prjId.setProject(prj);
					prjId.setProvenance(sources.get(0).getLabel());
					prjId.setVisibility(true);
			    	prjIds.add(prjId);
			    	
			    	prj.setProjectIdentifiers(prjIds);
			    }
					        	
	        	// project thematics
				List<Thematic> prjThematics = setProjectThematics(entitymanager, rowData, prjThematicsMap);
				if (prjThematics != null && prjThematics.size() > 0) {
					prj.setThematics(prjThematics);
				}									        	
								
				if (prjNumber != null) {
					entitymanager.persist(prj);
					projectMap.put(prjNumber, prj);
				}
        	}
       	        	
        }            
        
        System.out.println("Projects imported successfully.");
        
        return projectMap;
	}
	
	private Map<Organization, List<Project>> importOrganizations(List<Map<String, String>> data, EntityManager entitymanager, List<Source> sources, Map<String, Project> projectMap) {
		
		System.out.println("Starting importing organizations...");
		
		Map<Organization, List<Project>> orgProjectMap = new HashMap<>();					
		Set<String> extraFields = new HashSet<String>(Arrays.asList("promotion category", "project lead", "web project lead", "status", "keywords"));		
		Map<String, Organization> orgMap = new HashMap<>();		
		int tot_prj = 0;
		
		// read CSV file line by line
        for(int i=0; i<data.size(); i++) {
        	if ((i % 1000) == 0) {
        		System.out.println(i);
        	}
        	
        	Map<String, String> rowData = data.get(i);
        	
        	String prjNumber = rowData.get("project number");
        	Project prj = projectMap.get(prjNumber); 
			if (prj != null) {

				// set main organization fields        	
	        	List<Organization> orgs = setParentChildOrganizations(entitymanager, rowData, sources, orgMap);	        	        	
	        		        		
        		// connect organizations with project
        		if (orgs != null && orgs.size() > 0) {
        			
        			List<ProjectExtraField> prjExtraFields = setProjectExtraFields(rowData, extraFields, prj);
    				if (prjExtraFields != null && prjExtraFields.size() > 0) {
    					for (ProjectExtraField projectExtraField : prjExtraFields) {
    						entitymanager.persist(projectExtraField);
    					}
    				}
        			
        			
        			for (Organization org: orgs) {

        				List<Project> orgProjects = org.getProjects(); 
        				if (orgProjects != null) {
        					boolean insert = true;
        					for (Project p: orgProjects) {
        						if (p.getLabel().equals(prj.getLabel())) {
        							insert = false;
        							break;
        						}
        					}
        					
        					if (insert) {
        						orgProjects.add(prj);
        						orgProjectMap.put(org, orgProjects);
        						tot_prj+=orgProjects.size();
        					}
        				} else {
        					orgProjects = new ArrayList<>();
        					orgProjects.add(prj);
        					orgProjectMap.put(org, orgProjects);
        					tot_prj+=orgProjects.size();        				
        				}
        			}
        		}
			}
        }    
        
        System.out.println("Organizations imported successfully.");
        
        return orgProjectMap;
	}
	
	public static String getOrgProjectIdentifiers(Organization org) {
		
		// retrieve project identifiers
		List<Project> prjs = org.getProjects();

		String orgId = null;
		
		if (prjs != null) {
			List<String> ids = new ArrayList<>();
			for (Project p: prjs) {
				List<ProjectIdentifier> pIds = p.getProjectIdentifiers();
				if (pIds != null) {
					for(ProjectIdentifier pi: pIds) {
						ids.add(pi.getIdentifierName() + " " + pi.getIdentifier());
					}
				}
			}
			orgId = String.join(",", ids);
		}
		
		return orgId;
	}
	
	private void connectOrgWithProjects(EntityManager entitymanager, Map<Organization, List<Project>> orgProjectMap) {
		
		// connect organizations with projects 
		int tot_prj = 0;
		for (Organization org: orgProjectMap.keySet()) {
			List<Project> prjs = orgProjectMap.get(org);
			if (prjs != null) {
				org.setProjects(prjs);
				tot_prj+=prjs.size();								
				
				entitymanager.merge(org);
			}
		}
		
	}
	
	public static Source getSourceByName(EntityManager entitymanager, String sourceName) {
		
		// retrieve data source from database by name
		Query query = entitymanager.createQuery("Select s FROM Source s where s.label=:sourceLabel");
		query.setParameter("sourceLabel", sourceName);
		List<Source> sources = query.getResultList();
		Source source = sources.get(0);
		return source;
	}
	
	public static List<Organization> retrieveOrganizations(EntityManager entitymanager, Source source) {

		// retrieve from the database the organizations that appertain to some data source
		entitymanager.getTransaction().begin();
		Query query = entitymanager.createQuery("Select o FROM Organization o WHERE :id MEMBER OF o.sources");
		query.setParameter("id", source);
		List<Organization> orgs = query.getResultList();

		entitymanager.getTransaction().commit();
		System.out.println("Retrieved " + orgs.size() + " organizations");

		return orgs;
	}
	
	public void importData(String db) {
				
		// initialize entity manager factory
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();
						        
        entitymanager.getTransaction().begin();
		
        // get or create fwf source -------------------------------------------
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
    	
    	System.out.println("Starting importing data..."); 
    	
    	entitymanager.getTransaction().begin();
    	
    	Map<String, Project> projectMap = importProjects(this.projectData, entitymanager, sources);    	    	
    	
    	Map<Organization, List<Project>> orgProjectMap = importOrganizations(this.projectData, entitymanager, sources, projectMap);
    	connectOrgWithProjects(entitymanager, orgProjectMap);    	
    	
    	entitymanager.getTransaction().commit();    	 	
             
		entitymanager.close();
		emfactory.close();   
		
		emfactory = Persistence.createEntityManagerFactory(db);
		entitymanager = emfactory.createEntityManager();
		
		Source sourceFWF = getSourceByName(entitymanager, sourceName);
		List<Organization> orgs = retrieveOrganizations(entitymanager, source);
		
		entitymanager.getTransaction().begin();
		
		for (Organization org: orgs) {
    		    		
			// set unique identifier
			String identifier = sourceName + "::" + org.getLabel() + "::" + getOrgProjectIdentifiers(org);
			OrganizationIdentifier orgId = new OrganizationIdentifier();
			orgId.setIdentifier(identifier);
			orgId.setProvenance(sourceName);
			orgId.setIdentifierName("lid");
			orgId.setVisibility(false);
			orgId.setOrganization(org);
			entitymanager.persist(orgId);
    		
    	}
		
		entitymanager.getTransaction().commit();
		
		entitymanager.close();
		emfactory.close();
		
		System.out.println("Importer completed.");
		
        
	}

}

