package it.unimore.alps.genericimporter;

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

import com.opencsv.CSVReader;

import it.unimore.alps.sql.model.Link;
import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationActivity;
import it.unimore.alps.sql.model.OrganizationExtraField;
import it.unimore.alps.sql.model.OrganizationIdentifier;
import it.unimore.alps.sql.model.OrganizationType;
import it.unimore.alps.sql.model.Source;

public class GenericImporter {
	
	private String dataFile;							// name file containing data to be imported
	private CSVReader dataReader;						// file CSV reader
	List<String> dataSchema = new ArrayList<String>();	// CSV file schema

	public static void main(String[] args) {

		// BEGIN: INPUT PARAMETERS ------------------------------------------------------
		CommandLine commandLine;
		// name of the database where to insert the information
		Option DB = Option.builder("DB").hasArg().required(true).desc("DB. ").longOpt("DB").build();
		// CSV file containing the data to be imported
		Option dataOption = Option.builder("data").hasArg().required(true)
				.desc("The file that contains organizations data. ").longOpt("data").build();
		
		Options options = new Options();
		CommandLineParser parser = new DefaultParser();
		
		options.addOption(DB);
		options.addOption(dataOption);
		
		String db = null;
		String data = null;
		boolean header = true;
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
			
			if(commandLine.hasOption("data")) {       	
	        	data =commandLine.getOptionValue("data");
	        	System.out.println("Data file name: " + data);
	        } else {
	        	System.out.println("\tFile not provided. Use the data option.");
	        	System.exit(1);
	        }
			
		} catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// END: INPUT PARAMETERS --------------------------------------------------------
		
		// import data
		GenericImporter genericImporter = new GenericImporter(data, header);
		genericImporter.importData(db);

	}
	
	public GenericImporter(String dataFile, boolean header) {
		
		this.dataFile = dataFile;		
		// read CSV file and load in memory
		this.dataReader = initializeCSVReader(dataFile, header, dataSchema);

	}
	
	private CSVReader initializeCSVReader(String inputFile, boolean header, List<String> schema) {
		
		CSVReader reader = null;
		
		// load dataset in memory
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
	
	
	
	private Organization setOrganizationFields(Map<String,String> data, List<Source> sources) {
		
		// save CSV data into an Organization object		
		Organization org = new Organization();				
		
		String address = data.get("address");
	    if (!address.equals("")) {
	    	org.setAddress(address);	 
	    	org.setAddressSources(sources);
	    }
	    
	    String alias = data.get("alias");
	    if (!alias.equals("")) {
	    	org.setAlias(alias);
	    }
	    
	    String city = data.get("city");
	    if (!city.equals("")) {
	    	String cleanCity = city.substring(0,1).toUpperCase() + city.substring(1).toLowerCase();
	    	org.setCity(cleanCity);
	    }
	    
	    String cityCode = data.get("city_code");
	    if (!cityCode.equals("")) {
	    	org.setCityCode(cityCode);
	    }
	    
	    String commercialLabel = data.get("commercial_label");
	    if (!commercialLabel.equals("")) {
	    	org.setCommercialLabel(commercialLabel);
	    }
	    
	    String country = data.get("country");
	    if (!country.equals("")) {
	    	org.setCountry(country);
	    }
	    
	    String countryCode = data.get("country_code");
	    if (!countryCode.equals("")) {
	    	org.setCountryCode(countryCode);
	    }
	    
	    String creationYear = data.get("creation_year");
	    if (!creationYear.equals("")) {
	    	
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy");
			Date utilDate = null;
			try {
				utilDate = formatter.parse(creationYear);
			} catch (java.text.ParseException e) {
				e.printStackTrace();
			}
			if (utilDate != null) {
				org.setCreationYear(utilDate);
			}	    		    		    	
	    }   
	    
	    String financePrivateDate = data.get("finance_private_date");
		if (!financePrivateDate.equals("")) {
			
			DateFormat df = new SimpleDateFormat("dd-MM-yyyy");
		    
			Date utilDate;
			try {
				utilDate = df.parse(financePrivateDate);
				org.setFinancePrivateDate(utilDate);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
		}
	    
		String financePrivateEmployees = data.get("finance_private_employees");
	    if (!financePrivateEmployees.equals("")) {
	    	org.setFinancePrivateEmployees(financePrivateEmployees);
	    }
	    	    
	    String financePrivateRevenueRange = data.get("finance_private_revenue_range");
	    if (!financePrivateRevenueRange.equals("")) {
	    	org.setFinancePrivateRevenueRange(financePrivateRevenueRange);
	    }
	    
	    String isPublic = data.get("is_public");
		if (!isPublic.equals("")) {
			
			isPublic = isPublic.toLowerCase();
			
			if (isPublic.equals("true") || isPublic.equals("false")) {
				org.setIsPublic(isPublic); 
			} else {
				org.setIsPublic("undefined");
			}
		}	    	    	    
		
    	String label = data.get("label");
		if (!label.equals("")) {
			org.setLabel(label);
		}
		
		String lat = data.get("lat");
		if (!lat.equals("")) {
			
			try {
				org.setLat(Float.parseFloat(lat));
			} catch (Exception e) {
				e.printStackTrace();
			}						
		}
		
		String lon = data.get("lon");
		if (!lon.equals("")) {
			
			try {
				org.setLon(Float.parseFloat(lon));
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
		
		String nutsLevel1 = data.get("nutslevel1");
	    if (!nutsLevel1.equals("")) {
	    	org.setNutsLevel1(nutsLevel1);
	    }
	    
	    String nutsLevel2 = data.get("nutslevel2");
	    if (!nutsLevel2.equals("")) {
	    	org.setNutsLevel2(nutsLevel2);
	    }
	    
	    String nutsLevel3 = data.get("nutslevel3");
	    if (!nutsLevel3.equals("")) {
	    	org.setNutsLevel3(nutsLevel3);
	    }	    
	    
	    String cap = data.get("postcode");
	    if (!cap.equals("")) {
	    	org.setPostcode(cap);
	    }
	    	    
	    String typeCategoryCode = data.get("type_category_code");
	    if (!typeCategoryCode.equals("")) {
	    	org.setTypeCategoryCode(typeCategoryCode);
	    }
	    
	    String typeKind = data.get("type_kind");
	    if (!typeKind.equals("")) {
	    	org.setTypeKind(typeKind);
	    }
	    
	    String typeLabel = data.get("type_label");
	    if (!typeLabel.equals("")) {
	    	org.setTypeLabel(typeLabel);
	    }
	    
	    String urbanUnit = data.get("urban_unit");
	    if (!urbanUnit.equals("")) {
	    	org.setUrbanUnit(urbanUnit);
	    }
	    
	    String urbanUnitCode = data.get("urban_unit_code");
	    if (!urbanUnitCode.equals("")) {
	    	org.setUrbanUnitCode(urbanUnitCode);
	    }	     	    
	    
	    return org;
		
	}

	private List<Source> getSource(Map<String,String> data, Map<String, List<Source>> sourceMap) {
		
		// save information about data source
		List<Source> sources = new ArrayList<>();
		Source source = new Source();
		
		String label = data.get("source_label");
		if (!label.equals("")) {
			
			if (sourceMap.containsKey(label)) {
				return sourceMap.get(label);
			}
						
			source.setLabel(label);
		}
		
		String url = data.get("source_url");
		if (!url.equals("")) {
			source.setUrl(url);
		}
		
		String revisionData = data.get("source_revision_date");
		if (!revisionData.equals("")) {
			
			DateFormat df = new SimpleDateFormat("dd-MM-yyyy");
		    
			Date sourceDate;
			try {
				sourceDate = df.parse(revisionData);
				source.setRevisionDate(sourceDate);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
		}
		
		sources.add(source);
		sourceMap.put(source.getLabel(), sources);
		
		return sources;		
	}
	
	private List<Link> setOrganizationLinks(Map<String,String> data, String websiteKey) {
		
		// save website		
		List<Link> links = new ArrayList<>();
		
		String websites = data.get(websiteKey); 
		if (!websites.equals("")) {
			
			String[] websiteArray = websites.split(";");
			
			for (String website: websiteArray) {
				if (!website.equals("")) {
					Link link = new Link();
			    	link.setUrl(website);
			    	link.setType("main");
			    	link.setLabel("homepage");
			    	links.add(link);
			    }
			}
			
		}				
	    
	    return links;
	}
	
	private List<OrganizationExtraField> setOrganizationExtraFields(Map<String,String> data, Set<String> attributes, Organization org) {
		
		// save extract information
		List<OrganizationExtraField> orgExtraFields = new ArrayList<OrganizationExtraField>();		
		
		for (String attr : attributes) {
			if (!data.get(attr).equals("")) {
				OrganizationExtraField orgExtraField = new OrganizationExtraField();
				orgExtraField.setVisibility(true);
				orgExtraField.setFieldKey(attr);
				orgExtraField.setFieldValue(data.get(attr).trim());
				orgExtraField.setOrganization(org);
				orgExtraFields.add(orgExtraField);
			}
		}
		
		return orgExtraFields;
	}
		
	
	public void importData(String db) {
		
		// initialize entity manager factory
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();
		
		// load the input CSV file in memory as a map
		List<Map<String, String>> data = createMapFromFileCSV(dataReader, dataSchema);
		
		entitymanager.getTransaction().begin();
		Map<String, List<Source>> sourceMap = new HashMap<>();
		Set<String> attributes = new HashSet<String>(Arrays.asList("phone", "email", "description"));
		
		// read organization data and import it into the database 
		System.out.println("Starting importing organizations...");		

        for(int i=0; i < data.size(); i++) {
        	if ((i % 100) == 0) {
        		System.out.println(i);
        	}
        	
        	// file row data
        	Map<String, String> rowData = data.get(i);
        	
        	List<Source> sources = getSource(rowData, sourceMap);   		    	
        	
        	// save basic organization information
        	Organization org = setOrganizationFields(rowData, sources);         	
        	
        	if (org != null) {
        		
            	// set organization website
        		List<Link> links = setOrganizationLinks(rowData, "links");
	        	if (links != null) {
	        		for (Link link: links) {
		        		link.setSources(sources);
		        		entitymanager.persist(link);
	        		}
	        		
	        		org.setLinks(links);
	        		
	        	}        			        	
        		
        		// set organization sources
        		org.setSources(sources); 
        		
        		// set organization identifiers   
        		Source source = sources.get(0);
        		String sourceName = source.getLabel();
            	String identifier = sourceName + "::" + org.getLabel() + "::" + org.getAddress();            	
        		OrganizationIdentifier localOrgId = new OrganizationIdentifier();
        		localOrgId.setIdentifier(identifier);
        		localOrgId.setProvenance(sourceName);
        		localOrgId.setIdentifierName("lid");
        		localOrgId.setVisibility(false);
        		localOrgId.setOrganization(org);
        		
        		List<OrganizationIdentifier> orgIdentifiers = new ArrayList<>();
        		orgIdentifiers.add(localOrgId);

        		org.setOrganizationIdentifiers(orgIdentifiers);
        		
        		// set organization extra fields        		
        		List<OrganizationExtraField> orgExtraFields = setOrganizationExtraFields(rowData, attributes, org);
        		org.setOrganizationExtraFields(orgExtraFields);
        		
        		entitymanager.persist(org);
        		
        	}         	
        	
        }                
        
        entitymanager.getTransaction().commit();
        System.out.println("Imported " + data.size() + " organizations.");
        
		entitymanager.close();
		emfactory.close();     
		
		
	}
}
