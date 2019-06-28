package it.unimore.alps.sources.grid;

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

import it.unimore.alps.sources.grid.GridImporter;
import it.unimore.alps.sql.model.Link;
import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationExtraField;
import it.unimore.alps.sql.model.OrganizationIdentifier;
import it.unimore.alps.sql.model.OrganizationType;
import it.unimore.alps.sql.model.Source;


public class GridImporter {
	
	private CSVReader orgReader;												// organization file reader
	private CSVReader aliasReader;												// organization alias file reader
	private CSVReader addressReader;											// organization address file reader
	private CSVReader linkReader;												// organization website file reader
	private CSVReader typeReader;												// organization type file reader
	private CSVReader idReader;													// organization identifier file reader
	private CSVReader acronymReader;											// organization acronym file reader
	private CSVReader labelReader;												// organization label file reader
	private CSVReader relationshipReader;										// organization relationship file reader
	private CSVReader geonameReader;											// organization geo-coordinate file reader
	List<String> orgSchema = new ArrayList<String>();							// organization file schema
	List<String> aliasSchema = new ArrayList<String>();							// organization alias file schema
	List<String> addressSchema = new ArrayList<String>();						// organization address file schema
	List<String> linkSchema = new ArrayList<String>();							// organization website file schema
	List<String> typeSchema = new ArrayList<String>();							// organization type file schema
	List<String> idSchema = new ArrayList<String>();							// organization identifier file schema
	List<String> acronymSchema = new ArrayList<String>();						// organization acronym file schema
	List<String> labelSchema = new ArrayList<String>();							// organization label file schema
	List<String> relationshipSchema = new ArrayList<String>();					// organization relationship file schema
	List<String> geonameSchema = new ArrayList<String>();						// organization geo-coordinate file schema
	List<Map<String, String>> orgData = new ArrayList<>();						// organization data
	Map<String, List<Map<String, String>>> aliasData = new HashMap<>();			// organization alias data
	Map<String, List<Map<String, String>>> addressData = new HashMap<>();		// organization address data
	Map<String, List<Map<String, String>>> linkData = new HashMap<>();			// organization website data
	Map<String, List<Map<String, String>>> typeData = new HashMap<>();			// organization type data
	Map<String, List<Map<String, String>>> idData = new HashMap<>();			// organization identifier data
	Map<String, List<Map<String, String>>> acronymData = new HashMap<>();		// organization acronym data
	Map<String, List<Map<String, String>>> labelData = new HashMap<>();			// organization label data
	List<Map<String, String>> relationshipData = new ArrayList<>();				// organization relationship data
	Map<String, List<Map<String, String>>> geonameData = new HashMap<>();		// organization geo-coordinate data
	
	Map<String,OrganizationType> types=new HashMap<>();
	
	private String sourceName = "Grid";											// data source label
	private String sourceUrl = "https://www.grid.ac";							// data source url
	private String sourceRevisionDate = "06-05-2019";							// data source date
	
	public static void main(String[] args) {
		
		// BEGIN: INPUT PARAMETERS ------------------------------------------------------
		CommandLine commandLine;
		// organization file
		Option orgFileOption = Option.builder("orgFile")
	        	.hasArg()
		        .required(true)
		        .desc("The file that contains institutes names data.")
		        .longOpt("organizationFile")
		        .build();
		// organization alias file
		Option aliasFileOption = Option.builder("aliasFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains institutes aliases data.")
	            .longOpt("aliasFile")
	            .build();
		// organization address file
		Option addressFileOption = Option.builder("addressFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains institutes addresses and other data.")
	            .longOpt("addressFile")
	            .build();
		// organization website file
		Option linkFileOption = Option.builder("linkFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains links.")
	            .longOpt("linkFile")
	            .build();
		// organization type file
		Option typeFileOption = Option.builder("typeFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains the types of institutes.")
	            .longOpt("typeFile")
	            .build();
		// organization identifier file
		Option idFileOption = Option.builder("idFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains the external ids of institutes.")
	            .longOpt("idFile")
	            .build();
		// organization acronym file
		Option acronymOption = Option.builder("acronymFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains the acronyms of the institutes.")
	            .longOpt("acronymFile")
	            .build();
		// organization label file
		Option labelOption = Option.builder("labelFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains the label of institutes.")
	            .longOpt("labelFile")
	            .build();
		// organization relationship file
		Option relationshipOption = Option.builder("relationshipFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains the relationship between institutes.")
	            .longOpt("relationshipFile")
	            .build();
		// organization geo-coordinate file
		Option geonameOption = Option.builder("geonameFile")
        		.hasArg()
	            .required(true)
	            .desc("The file that contains the geonames information.")
	            .longOpt("geonamesImported")
	            .build();
		// entity type to be imported
		Option entityTypeOption = Option.builder("entityType")
        		.hasArg()
	            .required(true)
	            .desc("The entity type to be imported. For now there are only 'organization'.")
	            .longOpt("entityTypeImported")
	            .build();
        Option incrementalModeOption = Option.builder("incMode")
	            .required(false)
	            .desc("The incremental mode consists in importing only new data.")
	            .longOpt("incrementalMode")
	            .build();
        Option DB = Option.builder("DB").hasArg().required(true).desc("DB. ").longOpt("DB").build();
		Options options = new Options();
		CommandLineParser parser = new DefaultParser();
		
	    options.addOption(orgFileOption);
	    options.addOption(aliasFileOption);
	    options.addOption(addressFileOption);
	    options.addOption(linkFileOption);
	    options.addOption(typeFileOption);
	    options.addOption(idFileOption);
	    options.addOption(acronymOption);
	    options.addOption(labelOption);
	    options.addOption(relationshipOption);
	    options.addOption(geonameOption);
	    options.addOption(entityTypeOption);
        options.addOption(incrementalModeOption);
        options.addOption(DB);
        
        String orgFile = null;
        String aliasFile = null;
        String addressFile = null;
        String linkFile = null;
        String typeFile = null;
        String entityType = null;
        String idFile = null;
        String acronymFile = null;
        String labelFile = null;
        String relationshipFile = null;
        String geonameFile = null;
        boolean incrementalMode = false;
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
	        	System.out.println("\tOrganization file: " + orgFile);
			} else {
				System.out.println("\tOrganization file not provided. Use the orgFile option.");
	        	System.exit(1);
			}
	        
	        if (commandLine.hasOption("aliasFile")) {   
	        	aliasFile = commandLine.getOptionValue("aliasFile");
	        	System.out.println("\tAlias file: " + aliasFile);
			} else {
				System.out.println("\tAlias file not provided. Use the aliasFile option.");
	        	System.exit(1);
			}
	        
	        if (commandLine.hasOption("addressFile")) {   
	        	addressFile = commandLine.getOptionValue("addressFile");
	        	System.out.println("\tAddress file: " + addressFile);
			} else {
				System.out.println("\tAddress file not provided. Use the addressFile option.");
	        	System.exit(1);
			}
			
	        if (commandLine.hasOption("linkFile")) {   
	        	linkFile = commandLine.getOptionValue("linkFile");
	        	System.out.println("\tlink file: " + linkFile);
			} else {
				System.out.println("\tlink file not provided. Use the linkFile option.");
	        	System.exit(1);
			}
	        
	        if (commandLine.hasOption("typeFile")) {   
	        	typeFile = commandLine.getOptionValue("typeFile");
	        	System.out.println("\ttypes file: " + typeFile);
			} else {
				System.out.println("\ttypes file not provided. Use the typeFile option.");
	        	System.exit(1);
			}
	        
	        if (commandLine.hasOption("idFile")) {   
	        	idFile = commandLine.getOptionValue("idFile");
	        	System.out.println("\texternal ids file: " + idFile);
			} else {
				System.out.println("\texternal ids file not provided. Use the idFile option.");
	        	System.exit(1);
			}
	        
	        if (commandLine.hasOption("acronymFile")) {   
	        	acronymFile = commandLine.getOptionValue("acronymFile");
	        	System.out.println("\tacronym file: " + acronymFile);
			} else {
				System.out.println("\tacronymFile not provided. Use the acronymFile option.");
	        	System.exit(1);
			}
	        
	        if (commandLine.hasOption("labelFile")) {   
	        	labelFile = commandLine.getOptionValue("labelFile");
	        	System.out.println("\tlabel file: " + labelFile);
			} else {
				System.out.println("\tlabel file not provided. Use the labelFile option.");
	        	System.exit(1);
			}
	        
	        if (commandLine.hasOption("relationshipFile")) {   
	        	relationshipFile = commandLine.getOptionValue("relationshipFile");
	        	System.out.println("\trelationship file: " + relationshipFile);
			} else {
				System.out.println("\trelationship file not provided. Use the relationshipFile option.");
	        	System.exit(1);
			}
	        
	        if (commandLine.hasOption("geonameFile")) {   
	        	geonameFile = commandLine.getOptionValue("geonameFile");
	        	System.out.println("\tgeoname file: " + geonameFile);
			} else {
				System.out.println("\tgeoname file not provided. Use the geonameFile option.");
	        	System.exit(1);
			}
	        
	        if (commandLine.hasOption("entityType")) {   
	        	entityType = commandLine.getOptionValue("entityType");
	        	System.out.println("\tentityType: " + entityType);
			} else {
				System.out.println("\tentityType not provided. Use the entityType option.");
	        	System.exit(1);
			}
	        
			if (commandLine.hasOption("incMode")) {
				System.out.println("\tIncremental mode activated");
				incrementalMode = true;
			}
			
			System.out.println("----------------------------\n");
        } catch (org.apache.commons.cli.ParseException e) {
			e.printStackTrace();
		}
		// END: INPUT PARAMETERS --------------------------------------------------------
        
        // import data
		GridImporter gridImporter = new GridImporter(orgFile, aliasFile, addressFile, linkFile, typeFile, idFile, acronymFile, labelFile, relationshipFile, geonameFile, true);
		gridImporter.importData(entityType, incrementalMode, db);
	}
	
	public GridImporter(String orgFile, String aliasFile, String addressFile, String linkFile, String typeFile, String idFile, String acronymFile, String labelFile, String relationshipFile, String geonameFile, boolean header) { 

		// read and load in memory input CSV files
		this.orgReader = initializeCSVReader(orgFile, header, orgSchema);
		this.aliasReader = initializeCSVReader(aliasFile, header, aliasSchema);
		this.addressReader = initializeCSVReader(addressFile, header, addressSchema);
		this.linkReader = initializeCSVReader(linkFile, header, linkSchema);
		this.typeReader = initializeCSVReader(typeFile, header, typeSchema);
		this.idReader = initializeCSVReader(idFile, header, idSchema);
		this.acronymReader = initializeCSVReader(acronymFile, header, acronymSchema);
		this.labelReader = initializeCSVReader(labelFile, header, labelSchema);
		this.relationshipReader = initializeCSVReader(relationshipFile, header, relationshipSchema);
		this.geonameReader = initializeCSVReader(geonameFile, header, geonameSchema);

	}
	
	private CSVReader initializeCSVReader(String inputFile, boolean header, List<String> schema) {
			
		CSVReader reader = null;
		
		// load in memory CSV file
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
	
	public static String getOrgIds(List<OrganizationIdentifier> orgIds) {
		
		// retrieve organization identifiers
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
	
	private Map<String,Organization> importOrganizations(EntityManager entitymanager, List<Source> sources, boolean incrementalMode) {
			
		this.orgData = createMapFromFileCSV(orgReader, orgSchema);	
		this.aliasData = createHashMapFromFileCSV(aliasReader, aliasSchema);
		this.addressData = createHashMapFromFileCSV(addressReader, addressSchema);
		this.linkData = createHashMapFromFileCSV(linkReader, linkSchema);
		this.typeData = createHashMapFromFileCSV(typeReader, typeSchema);
		this.idData = createHashMapFromFileCSV(idReader, idSchema);
		this.acronymData = createHashMapFromFileCSV(acronymReader, acronymSchema);
		this.labelData = createHashMapFromFileCSV(labelReader, labelSchema);
		this.geonameData = createHashMapFromFileCSV(geonameReader, geonameSchema);
		
		// read organization data and import it into the database
		System.out.println("Starting importing organizations...");
		
		entitymanager.getTransaction().begin();
        Map<String,Organization> orgMap = new HashMap<String,Organization>(); 
        List<Map<String, String>> orgDataTMP = new ArrayList<>();
        for(int i=0;i<orgData.size();i++) {
        	orgDataTMP.add(orgData.get(i));
        }
        
        // read CSV file line by line
        for(int i=0; i<orgDataTMP.size(); i++) {
        	if ((i % 1000) == 0) {
        		System.out.println(i);
        	}
        	
        	// CSV data row
        	Map<String, String> rowData= orgDataTMP.get(i);

        	String country="";
        	country=retrieveField(addressData, rowData.get("grid_id"), "country_code");
        	
        	if(country.equalsIgnoreCase("IT") || country.equalsIgnoreCase("FR") || country.equalsIgnoreCase("DE") || country.equalsIgnoreCase("AT") || country.equalsIgnoreCase("SI") || country.equalsIgnoreCase("CH") || country.equalsIgnoreCase("LI"))
        	{
	        	// set main organization fields
	        	Organization org = setOrganizationFields(rowData, sources);
	        	if (org != null) {
	        		org.setSources(sources);
		        	orgMap.put(rowData.get("grid_id"), org);
	        	}
	        	
	        	List<Link> links = new ArrayList<>();
	        	links=setLinkFields(rowData, sources, org.getId());
   		        if (links.size() > 0) {
	        		for(int j=0;j<links.size();j++) {
	        			links.get(j).setSources(sources);
		        		entitymanager.persist(links.get(j));
	        		}
	        		org.setLinks(links);
	        	}
	        	
	        	OrganizationType type = setTypeFields(rowData, sources, org.getId()); 
	        	if (type != null) {
	        		if(!types.containsKey(type.getLabel())) {
	        			types.put(type.getLabel(), type);
		        		entitymanager.persist(type);
	        		} else {
	        			type = types.get(type.getLabel());
	        		}
	        		org.setOrganizationType(type);
	        	}
	        	
	        	List<OrganizationIdentifier> identifiers = new ArrayList<>(); 
	        	identifiers = setIdentifierFields(rowData, sources, org.getId(), org); 
	        	if (identifiers.size() > 0) {
	        		for(int j=0;j<identifiers.size();j++)
		        	{
		        		entitymanager.persist(identifiers.get(j));
	        		}
	        		org.setOrganizationIdentifiers(identifiers);
	        		
	        		// set unique identifier
					String identifier = sourceName + "::" + getOrgIds(identifiers);
					OrganizationIdentifier orgId = new OrganizationIdentifier();
					orgId.setIdentifier(identifier);
					orgId.setProvenance(sourceName);
					orgId.setIdentifierName("lid");
					orgId.setVisibility(false);
					orgId.setOrganization(org);
					entitymanager.persist(orgId);
	        	}

	        	Map<String, String> extraMap = new HashMap();
	        	rowData.put("geonames_city_id", org.getCityCode());
	        	extraMap=setExtraFields(rowData, sources, org.getId());
	        	OrganizationExtraField extra;
	        	List<OrganizationExtraField> extraList=new ArrayList();
	        	if(extraMap.size()>0) {
	        		for(String key : extraMap.keySet())
	        		{
	        			extra = new OrganizationExtraField();
	        			extra.setFieldKey(key);
	        			extra.setFieldValue(extraMap.get(key));
	        			extra.setOrganization(org);
	        			extra.setVisibility(true);
	        			extraList.add(extra);
		        		entitymanager.persist(extra);
	        		}
	        		org.setOrganizationExtraFields(extraList);
	        	}
        		entitymanager.persist(org);
        	}
        }
        
        entitymanager.getTransaction().commit();
        
        System.out.println("Imported " + orgMap.size() + " organizations");
        
        return orgMap;
	}
	
	public Map<String,Organization> importChildren(EntityManager entitymanager, Map<String,Organization> orgMap, List<Source> sources) {
		
		// import information about child organizations
		this.relationshipData = createMapFromFileCSV(relationshipReader, relationshipSchema);
		Organization org;
		Map<String,Organization> tmpMap=orgMap;
		System.out.println("Starting importing children...");
		
		entitymanager.getTransaction().begin();
		int i=0;
		
		// read CSV file line by line
		for(String key : tmpMap.keySet()) {
        	if ((i % 1000) == 0) {
        		System.out.println(i);
        	}
        	
        	// CSV data row
        	Map<String, String> rowData= relationshipData.get(i);
        	List<Organization> children = setChildrenFields(rowData, sources, orgMap, key);
        	if (children.size() > 0) {
        		org=orgMap.get(key);
        		org.setChildrenOrganizations(children);
        		entitymanager.persist(org);
        	}
        	i++;
		}
    	entitymanager.getTransaction().commit();
		return orgMap;
	}
	
	public int findIndex(String id, List<Map<String, String>> data) {
		
		// find row with input grid identifier
		int index=-1;
		for(int i=0;i<data.size();i++) {
			if(data.get(i).get("grid_id").equals(id)) {
				index=i;
				break;
			}
		}
		return index;
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
	
	private Map<String, List<Map<String, String>>> createHashMapFromFileCSV(CSVReader reader, List<String> schema) {
		
		Map<String, List<Map<String, String>>> fullData = new HashMap<>();
		
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
	        	String connectId = null;
	        	if (schema.contains("nuts_level1_code")) {
	        		connectId = rowData.get("geonames_city_id");
	        	} else {
	        		connectId = rowData.get("grid_id");
	        	}
	        	
	        	if (connectId != null && !connectId.equals("")) { 
	        		if (fullData.get(connectId) != null) {
	        			List<Map<String, String>> rows = fullData.get(connectId);
	        			rows.add(rowData);
	        			fullData.put(connectId, rows);
	        		} else {
	        			List<Map<String, String>> rows = new ArrayList<>();
	        			rows.add(rowData);
	        			fullData.put(connectId, rows);
	        		}	        		
	        	}
	        	
	        }
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	    
	    return fullData;
	    
	}

	private Organization setOrganizationFields(Map<String,String> data, List<Source> sources) {
		
		// save organization information in Organization object
		Organization org = new Organization();	
    	String label = data.get("name");	
		if (!label.equals("")) {
			org.setLabel(label);
		}
		
		String startDateString = data.get("established");
		if (startDateString != null && !startDateString.equals("")) {
			DateFormat df = new SimpleDateFormat("yyyy");
	        
			Date startDate;
			try {
				startDate = df.parse(startDateString);
				org.setCreationYear(startDate);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		
		String alias="";
		alias=retrieveField(aliasData, data.get("grid_id"), "alias");
		if (!alias.isEmpty()) {
			org.setAlias(alias);
		}
		
		String city="";
		city=retrieveField(addressData, data.get("grid_id"), "city");
		if (!city.isEmpty()) {
			String cleanCity = getCleanedString(city);
			org.setCity(cleanCity);
		}
		
		/*String geonameId = retrieveField(addressData, data.get("grid_id"), "geonames_city_id");
		if (geonameId != null) {
			String urbanUnit="";
			urbanUnit=retrieveField(geonameData, geonameId, "nuts_level3_name");
			if (!urbanUnit.isEmpty()) {
				org.setUrbanUnit(urbanUnit);
			}
		}*/
		
		String lat="";
		lat=retrieveField(addressData, data.get("grid_id"), "lat");
		if (!lat.isEmpty()) {
			org.setLat(Float.valueOf(lat));
		}
		
		org.setAddressSources(sources);
		
		String lng="";
		lng=retrieveField(addressData, data.get("grid_id"), "lng");
		if (!lng.isEmpty()) {
			org.setLon(Float.valueOf(lng));
		}
		
		String country="";
		country=retrieveField(addressData, data.get("grid_id"), "country");
		if (!country.isEmpty()) {
			org.setCountry(country);
		}
		
		String countryCode="";
		countryCode=retrieveField(addressData, data.get("grid_id"), "country_code");
		if (!countryCode.isEmpty()) {
			org.setCountryCode(countryCode);
		}
		
		String postcode="";
		postcode=retrieveField(addressData, data.get("grid_id"), "postcode");
		if (!postcode.isEmpty()) {
			org.setPostcode(postcode);
		}
		
		String acronym="";
		List<String> acronyms=new ArrayList<String>();
		acronym=retrieveField(acronymData, data.get("grid_id"), "acronym");
		if (!acronym.isEmpty()) {
			acronyms.add(acronym);
		}
		if(acronyms.size()>0) {
			org.setAcronyms(acronyms);
		}
		
		String address="";
		address=retrieveField(addressData, data.get("grid_id"), "line_1");
		if (!address.isEmpty()) {
			org.setAddress(address);
			org.setAddressSources(sources);
		}else {
			address=retrieveField(addressData, data.get("grid_id"), "line_2");
			if (!address.isEmpty()) {
				org.setAddress(address);
				org.setAddressSources(sources);
			}else {
				address=retrieveField(addressData, data.get("grid_id"), "line_3");
				if (!address.isEmpty()) {
					org.setAddress(address);
					org.setAddressSources(sources);
				}
			}
		}
		
		String cityCode="";
		cityCode=retrieveField(addressData, data.get("grid_id"), "geonames_city_id");
		if(!cityCode.isEmpty()) {
			org.setCityCode(cityCode);	
		}
		
	    return org;
	}

	private List<Link> setLinkFields(Map<String,String> data, List<Source> sources, int id) {
		
		// save organization website information into Link objects
		List<Link> links = new ArrayList<>();
		Link link = new Link();
		String url = "";
		url=retrieveField(linkData, data.get("grid_id"), "link");
		if (!url.equals("")) {
			link.setId(id);
			link.setUrl(url);
			link.setLabel("homepage");
			link.setType("main");
			links.add(link);
		}
		link = new Link();
		url = "";
		url=data.get("wikipedia_url");
		if (!url.equals("")) {
			link.setId(id);
			link.setUrl(url);
			link.setLabel("Wikipedia page");
			link.setType("wikipedia");
			links.add(link);
		}
		
		return links;
	}

	private String getCleanedString(String s) {
		// capitalize first letter of the input string
		return s.substring(0,1).toUpperCase() + s.substring(1).toLowerCase();
	}

	private OrganizationType setTypeFields(Map<String,String> data, List<Source> sources, int id) {
		
		// save organization type information into OrganizationType object
		OrganizationType orgType = new OrganizationType();
			
		String type = "";
		type=retrieveField(typeData, data.get("grid_id"), "type");
		if (!type.equals("")) {
			type = getCleanedString(type);
			orgType.setLabel(type);
		}
		
		return orgType;
	}

	private List<OrganizationIdentifier> setIdentifierFields(Map<String,String> data, List<Source> sources, int id, Organization org) {
		
		// save organization identifier information into OrganizationIdentifier object
		List<OrganizationIdentifier> orgIds = new ArrayList<>();
		OrganizationIdentifier orgId = new OrganizationIdentifier();
			
		String idName = "";
		String identifier="";
		Map<String,String> mapId=retrieveIdentifiers(idData, data.get("grid_id"));
		orgId.setLink("https://www.grid.ac/institutes/"+data.get("grid_id"));
		orgId.setIdentifierName("Grid");
		orgId.setIdentifier(data.get("grid_id"));
		orgId.setProvenance("Grid");
		orgId.setOrganization(org);
		orgIds.add(orgId);
	    if (mapId.size()>0) {
	    	
	    	for(String key : mapId.keySet()) {
	    		OrganizationIdentifier extOrgId = new OrganizationIdentifier();
	    		identifier=key;
	    		idName=mapId.get(key);
	    		extOrgId.setIdentifier(identifier);
	    		extOrgId.setIdentifierName(idName);
	    		extOrgId.setOrganization(org);
	    		extOrgId.setProvenance("Grid");
				if(idName.equalsIgnoreCase("ISNI")) {
					extOrgId.setLink("http://www.isni.org/isni/"+identifier);
				}
				if(idName.equalsIgnoreCase("FundRef")) {
					extOrgId.setLink("https://search.crossref.org/funding?q="+identifier);
				}
				if(idName.equalsIgnoreCase("Wikidata")) {
					extOrgId.setLink("https://www.wikidata.org/wiki/"+identifier);
				}
				orgIds.add(extOrgId);
			}
		}
		
		return orgIds;
	}

	private Map<String,String> setExtraFields(Map<String,String> data, List<Source> sources, int id){
		
		// save organization extra information into OrganizationExtraField objects
		Map<String,String> extra = new HashMap();
		String state="";
		state=retrieveField(addressData, data.get("grid_id"), "state");
		if(!state.isEmpty()) {
			extra.put("state", state);	
		}
		String stateCode="";
		stateCode=retrieveField(addressData, data.get("grid_id"), "state_code");
		if(!stateCode.isEmpty()) {
			extra.put("state_code", stateCode);	
		}
		String nuts_level1_code="";
		nuts_level1_code=retrieveGeonameField(geonameData, data.get("geonames_city_id"), "nuts_level1_code");
		if(!nuts_level1_code.isEmpty()) {
			extra.put("nuts_level1_code", nuts_level1_code);	
		}
		String nuts_level1_name="";
		nuts_level1_name=retrieveGeonameField(geonameData, data.get("geonames_city_id"), "nuts_level1_name");
		if(!nuts_level1_name.isEmpty()) {
			extra.put("nuts_level1_name", nuts_level1_name);	
		}
		String nuts_level2_code="";
		nuts_level2_code=retrieveGeonameField(geonameData, data.get("geonames_city_id"), "nuts_level2_code");
		if(!nuts_level2_code.isEmpty()) {
			extra.put("nuts_level2_code", nuts_level2_code);	
		}
		String nuts_level2_name="";
		nuts_level2_name=retrieveGeonameField(geonameData, data.get("geonames_city_id"), "nuts_level2_name");
		if(!nuts_level2_name.isEmpty()) {
			extra.put("nuts_level2_name", nuts_level2_name);	
		}
		String nuts_level3_code="";
		nuts_level3_code=retrieveGeonameField(geonameData, data.get("geonames_city_id"), "nuts_level3_code");
		if(!nuts_level3_code.isEmpty()) {
			extra.put("nuts_level3_code", nuts_level3_code);
		}
		String nuts_level3_name="";
		nuts_level3_name=retrieveGeonameField(geonameData, data.get("geonames_city_id"), "nuts_level3_name");
		if(!nuts_level3_name.isEmpty()) {
			extra.put("nuts_level3_name", nuts_level3_name);	
		}
		String geonames_admin1_code="";
		geonames_admin1_code=retrieveGeonameField(geonameData, data.get("geonames_city_id"), "geonames_admin1_code");
		if(!geonames_admin1_code.isEmpty()) {
			extra.put("geonames_admin1_code", stateCode);	
		}
		String geonames_admin1_name="";
		geonames_admin1_name=retrieveGeonameField(geonameData, data.get("geonames_city_id"), "geonames_admin1_name");
		if(!geonames_admin1_name.isEmpty()) {
			extra.put("geonames_admin1_name", geonames_admin1_name);	
		}
		String geonames_admin1_ascii_name="";
		geonames_admin1_ascii_name=retrieveGeonameField(geonameData, data.get("geonames_city_id"), "geonames_admin1_ascii_name");
		if(!geonames_admin1_ascii_name.isEmpty()) {
			extra.put("geonames_admin1_ascii_name", geonames_admin1_ascii_name);	
		}
		String geonames_admin2_code="";
		geonames_admin2_code=retrieveGeonameField(geonameData, data.get("geonames_city_id"), "geonames_admin2_code");
		if(!geonames_admin2_code.isEmpty()) {
			extra.put("geonames_admin2_code", geonames_admin2_code);	
		}
		String geonames_admin2_name="";
		geonames_admin2_name=retrieveGeonameField(geonameData, data.get("geonames_city_id"), "geonames_admin2_name");
		if(!geonames_admin2_name.isEmpty()) {
			extra.put("geonames_admin2_name", geonames_admin2_name);	
		}
		String geonames_admin2_ascii_name="";
		geonames_admin2_ascii_name=retrieveGeonameField(geonameData, data.get("geonames_city_id"), "geonames_admin2_ascii_name");
		if(!geonames_admin2_ascii_name.isEmpty()) {
			extra.put("geonames_admin2_ascii_name", geonames_admin2_ascii_name);	
		}
		return extra;
	}

	public Map<String,String> retrieveIdentifiers(Map<String, List<Map<String,String>>> data, String id) {

		// retrieve organization identifiers
		Map<String,String> ret=new HashMap<String,String>();

		if (data.get(id) != null) {
			for(Map<String,String> map: data.get(id)) {
				ret.put(map.get("external_id"),map.get("external_id_type"));
			}
		}
		return ret;
	}

	public List<Organization> setChildrenFields(Map<String,String> data, List<Source> sources, Map<String,Organization> orgMap, String key){
		
		// save organization parent-child relationships
		Organization org=new Organization();
		List<Organization> orgList = new ArrayList();
		List<String> childrenIds=new ArrayList<String>();
		
		// finding child organization identifiers
		childrenIds=retrieveChildren(relationshipData, key);
		if(childrenIds.size()>0) {
			for(int i=0;i<childrenIds.size();i++) {
				orgList.add(orgMap.get(childrenIds.get(i)));
			}
		}
		return orgList;
	}

	public List<String> retrieveChildren(List<Map<String,String>> data, String id) {
		
		// retrieve child organization
		List<String> ret=new ArrayList<String>();
		Map<String,String> map = new HashMap<String,String>(); 
		for(int i=0;i<data.size();i++) {
			map=data.get(i);
			if(map.get("grid_id").equals(id) && map.get("relationship_type").equalsIgnoreCase("Child")) {
				ret.add(map.get("related_grid_id"));
			}
		}

		return ret;
	}

	public String retrieveField(Map<String, List<Map<String, String>>> data, String id, String field) {
		
		// retrieve the attribute values of an organization identified by id 
		String ret=""; 
		boolean flag=false;
	
		if (data.get(id) != null) {
			for(Map<String,String> map: data.get(id)) {
				ret=map.get(field);
			}
		}
		return ret;
	}

	public String retrieveGeonameField(Map<String, List<Map<String,String>>> data, String id, String field) {
		
		// retrieve organization geo-coordinate information
		String ret=""; 
		boolean flag=false;
		
		if (data.get(id) != null) {
			for(Map<String,String> map: data.get(id)) {
				ret=map.get(field);
			}
		}
	
		return ret;
	}

	public void importData(String entity, boolean incrementalMode, String db) {
		// initialize entity manager factory
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();
						        
        // get or create grid source -------------------------------------------
        entitymanager.getTransaction().begin();
        Query query = entitymanager.createQuery("Select s FROM Source s where s.label = :label");
        query.setParameter("label", "Grid");
        List<Source> gridSource = query.getResultList();
        Source source = null;
        if (gridSource.size() > 0) {
        	source = gridSource.get(0);
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
    	
    	Query queryType = entitymanager.createQuery("Select t FROM OrganizationType t");        
        List<OrganizationType> qTypes = queryType.getResultList();
        if (qTypes != null && qTypes.size() > 0) {
        	for(OrganizationType t: qTypes) {
        		types.put(t.getLabel(), t);
        	}
        }
    	
    	if (entity.equals("organization")) {			
    		Map<String,Organization> orgMap = importOrganizations(entitymanager, sources, incrementalMode);
    		orgMap=importChildren(entitymanager, orgMap, sources);	
		}
    	System.out.println("Grid importer completed.");
		entitymanager.close();
		emfactory.close(); 
	}	
}