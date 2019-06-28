package it.unimore.alps.sources.p3;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.nio.file.*;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
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
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.opencsv.*;

import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationExtraField;
import it.unimore.alps.sql.model.OrganizationIdentifier;
import it.unimore.alps.sql.model.OrganizationType;
import it.unimore.alps.sql.model.Project;
import it.unimore.alps.sql.model.ProjectExtraField;
import it.unimore.alps.sql.model.ProjectIdentifier;
import it.unimore.alps.sql.model.Publication;
import it.unimore.alps.sql.model.PublicationIdentifier;
import it.unimore.alps.sql.model.Source;
import it.unimore.alps.sql.model.Theme;
import it.unimore.alps.sql.model.Thematic;
import it.unimore.alps.sql.model.Person;
import it.unimore.alps.sql.model.PersonExtraField;
import it.unimore.alps.sql.model.PersonIdentifier;

public class P3Importer {

	/*
	 *  Files to be integrated: 
     *	P3_GrantExport.csv: projects
     *	P3_GrantExport_with_abstracts.csv: project with abstract
     *	P3_PersonExport.csv: people
     *	P3_PublicationExport.csv: publications
     *	P3_GrantOutputDataExport.csv: results
     *	P3_CollaborationExport.csv: collaborations
	 * 
	*/
	
	private String projectFile; 													// project file
	private String projectAbstractFile; 											// project with abstract file
	private String peopleFile; 														// people file
	private String publicationFile; 												// publication file
	private String outDataFile; 													// result file
	private String collaborationFile; 												// collaboration file
	
	private CSVReader projectReader;												// project file reader
	private CSVReader projectAbstractReader;										// project (with abstract) file reader
	private CSVReader peopleReader; 												// people file reader
	private CSVReader publicationReader; 											// publication file reader
	private CSVReader outDataReader; 												// result file reader
	private CSVReader collaborationReader;											// collaboration file reader
	private CSVReader organizationReader;											// organization file reader
	
	// Building the common CSV parser using JavaBean notation.
	private CSVParser parserCSV = new CSVParserBuilder()
									  .withSeparator(';')
									  .build();
	
	List<String> projectSchema = new ArrayList<String>();							// project file schema
	List<String> peopleSchema = new ArrayList<String>();							// people file schema
	List<String> publicationSchema = new ArrayList<String>();						// publication file schema
	List<String> outDataSchema = new ArrayList<String>();							// result file schema
	List<String> collaborationSchema = new ArrayList<String>();						// collaboration file schema

	
	List<Map<String, String>> projectData = new ArrayList<>();						// project data
	List<Map<String, String>> orgData = new ArrayList<>();							// organization data
	List<Map<String, String>> publicationData = new ArrayList<>();					// publication data
	List<Map<String, String>> peopleData = new ArrayList<>();						// people data
	List<Map<String, String>> authorsData = new ArrayList<>();						// author data
	Map<String, String> peopleOrgsData = new HashMap<String, String>();				// person-organization data  
	
 	private String sourceName = "P3";												// data source name
	private String sourceUrl = "http://p3.snf.ch/Pages/DataAndDocumentation.aspx";	// data source url
	private String sourceRevisionDate = "01-06-2019"; 								// data source date
	
	public static final String UTF8_BOM = "\uFEFF";
	
	
	public static void main(String[] args) {
		
		// BEGIN: INPUT PARAMETERS ------------------------------------------------------		
		CommandLine cmdLine;
		// project file
		Option projectFileOption = Option.builder("prjFile")
				.hasArg()
				.required(true)
				.desc("The file containing projects (main grants) data.")
				.longOpt("projectFile")
				.build();
		// person file
		Option peopleFileOption = Option.builder("pplFile")
				.hasArg()
				.required(true)
				.desc("The file containing people data.")
				.longOpt("peopleFile")
				.build();
		// publication file
		Option publicationFileOption = Option.builder("pubFile")
				.hasArg()
				.required(true)
				.desc("The file containing publications data.")
				.longOpt("publicationFile")
				.build();
		// result file
		Option outDataFileOption = Option.builder("outFile")
				.hasArg()
				.required(true)
				.desc("the file containing projects output data.")
				.longOpt("OutDataFile")
				.build();
		// collaboration file
		Option collaborationFileOption = Option.builder("collabFile")
				.hasArg()
				.required(true)
				.desc("the file containing collaborations data.")
				.longOpt("collaborationFile")
				.build();
		// project (with abstract) file
		Option projectAbstractFileOption = Option.builder("hasAbstract")
				.required(false)
				.desc("The projects file has abstracts.")
				.longOpt("hasAbstract")
				.build();
		// entity type to be imported
		Option entityTypeOption = Option.builder("entityType")
        		.hasArg()
	            .required(true)
	            .desc("The entity type to be imported. The possibile choiches are: "
	            		+ "'all', 'project', 'organization', 'person', 'publication', 'output', 'collaboration' .")
	            .longOpt("entityTypeImported")
	            .build();
		// incremental modality
        Option incrementalModeOption = Option.builder("incMode")
	            .required(false)
	            .desc("The incremental mode consists in importing only new data.")
	            .longOpt("incrementalMode")
	            .build();
        // debug mode
        Option debugModeOption = Option.builder("debugMode")
	            .required(false)
	            .desc("The debug mode is verbose-like and waits for input before adding new data.")
	            .longOpt("debugMode")
	            .build();
        // database where to import data
        Option DB = Option.builder("DB").hasArg().required(true).desc("DB. ").longOpt("DB").build();
        Options options = new Options();
        
        options.addOption(projectFileOption);
        options.addOption(peopleFileOption);
        options.addOption(publicationFileOption);
        options.addOption(outDataFileOption);
        options.addOption(collaborationFileOption);
        options.addOption(projectAbstractFileOption);
        options.addOption(entityTypeOption);
        options.addOption(incrementalModeOption);
        options.addOption(debugModeOption);
        options.addOption(DB);
		       
        CommandLineParser parser = new DefaultParser();
        
        String projectFile = null; 		
        String peopleFile = null; 			
        String publicationFile = null; 	
        String outDataFile = null; 		
        String collaborationFile = null; 
        String entityType = null;
        boolean hasAbstract = false;
        boolean incrementalMode = false;
        boolean header = true;
        boolean debugMode = false;
        String db = null;
        
        try {
			cmdLine = parser.parse(options, args);
						
			System.out.println("+---+---+---+---+---+---+---+");
			System.out.println("Selected options:");
			
			if(cmdLine.hasOption("DB")) {       	
	        	db =cmdLine.getOptionValue("DB");
	        	System.out.println("DB name: " + db);
	        } else {
	        	System.out.println("\tDB name not provided. Use the DB option.");
	        	System.exit(1);
	        }

	        if (cmdLine.hasOption("prjFile")) {   
	        	projectFile = cmdLine.getOptionValue("prjFile");
	        	System.out.print("\tProjects file: " + projectFile);
	        	
	        	if (cmdLine.hasOption("hasAbstract")) {
					System.out.print(", with abstracts.");
					hasAbstract = true;
				}
	        	System.out.println();
			} else {
				System.out.println("\tProjects file not provided. Use the 'prjFile' option.");
	        	System.exit(1);
			}
			
			if (cmdLine.hasOption("pplFile")) {
				peopleFile = cmdLine.getOptionValue("pplFile");
				System.out.println("\tPeople file: " + peopleFile);
			} else {
				System.out.println("\tPeople file not provided. Use the 'pplFile' option.");
	        	System.exit(1);
			}
	
			if (cmdLine.hasOption("pubFile")) {
				publicationFile = cmdLine.getOptionValue("pubFile");
				System.out.println("\tPublication file: " + publicationFile);
			} else {
				System.out.println("\tFile containing publications data not provided. Use the 'pubFile' option.");
				System.exit(1);
			}  			

			if (cmdLine.hasOption("outFile")) {
				outDataFile = cmdLine.getOptionValue("outFile");
				System.out.println("\tOutput data file: " + outDataFile);
			} else {
				System.out.println("\tFile containing output data not provided. Use the 'outFile' option.");
				System.exit(1);
			}  		
			
			if (cmdLine.hasOption("collabFile")) {
				collaborationFile = cmdLine.getOptionValue("collabFile");
				System.out.println("\tCollaborations file: " + collaborationFile);
			} else {
				System.out.println("\tFile containing collaborations data not provided. Use the 'collabFile' option.");
				System.exit(1);
			}  		
			
			if (cmdLine.hasOption("entityType")) {
				entityType = cmdLine.getOptionValue("entityType");
				System.out.println("\tEntity type to be exported: " + entityType);
				
				if (!entityType.equals("all") 			&& 
					!entityType.equals("project") 	 	&& 
					!entityType.equals("people") 	 	&& 
					!entityType.equals("publication")	&& 
					!entityType.equals("output")	 	&&
					!entityType.equals("collaboration") &&
					!entityType.equals("organization") 	&&
					!entityType.equals("linkPrjsPubs") 	  ) 
				{
					System.out.println("\tWrong entity type value provided. Only the following entity type values are allowed: "
										+ "'all', 'project', 'organization', 'people', 'publication', 'output', 'collaboration', 'linkPrjsPubs'.");
					System.exit(1);
				}
				
			} else {
				System.out.println("\tEntity type to be imported not provided. Use the 'entityType' option.");
				System.exit(1);
			}
			
			if (cmdLine.hasOption("incMode")) {
				System.out.println("\tIncremental mode activated");
				incrementalMode = true;
			}
			
			if (cmdLine.hasOption("debugMode")) {
				System.out.println("\tDebug mode activated");
				debugMode = true;
			}
			
			System.out.println("+---+---+---+---+---+---+---+\n");
        
    	} catch (org.apache.commons.cli.ParseException e) {
    		 e.printStackTrace();
    	}
		// END: INPUT PARAMETERS --------------------------------------------------------
        
        // import data
        P3Importer p3Importer = new P3Importer(projectFile, peopleFile, publicationFile, outDataFile, collaborationFile, header);
        p3Importer.importData(entityType, incrementalMode, hasAbstract,debugMode, db);
        
	}
	
	public P3Importer(String projectFile, String peopleFile, String publicationFile, String outDataFile, String collaborationFile, 
			  boolean header ) {

		// read and load in memory the input CSV files
		this.projectFile = projectFile;
		this.peopleFile = peopleFile;
		this.publicationFile = publicationFile;
		this.outDataFile = outDataFile;
		this.collaborationFile = collaborationFile;
		
		this.projectReader = initCSVReader(projectFile,header,projectSchema);
		this.peopleReader = initCSVReader(peopleFile,header,peopleSchema);
		this.publicationReader = initCSVReader(publicationFile,header,publicationSchema);
		this.outDataReader = initCSVReader(outDataFile,header,outDataSchema);
		this.collaborationReader = initCSVReader(collaborationFile,header,collaborationSchema);
		this.organizationReader = initCSVReader(projectFile,header,projectSchema);
	
	}

	private static String removeUTF8BOM(String s) {
	    if (s.startsWith(UTF8_BOM)) {
	        s = s.substring(1);
	    }
	    return s;
	}

	private CSVReader initCSVReader(String inputFile, boolean header, List<String> schema) {
		
		System.out.println("Cleaning file: "+inputFile+"...");
		
		replaceSeparatorCharacterInFieldsCSV(inputFile);
		
		CSVReader reader = null;
				
		try {
			reader = new CSVReaderBuilder(new InputStreamReader(new FileInputStream(inputFile), "UTF-8"))
				     .withCSVParser(new CSVParserBuilder()
				    		       .withSeparator(';')
				    		       .withIgnoreQuotations(true)
				    		       .withQuoteChar('\"')
				    		       .build())
				     .build();
			
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
		
		
		if( reader != null )
			if( header == true ) {
			try {
				String []l;
				if((l = reader.readNext()) != null )
					for(String item : l) {
						item = removeUTF8BOM(item);
						schema.add(item);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		
		return reader;
	}		
	
	private Source setSource() {
		
		// save data source information
		Source source = new Source();
		
		source.setLabel(this.sourceName);
		source.setUrl(this.sourceUrl);
		DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
        
		Date sourceDate;
		try {
			sourceDate = dateFormat.parse(this.sourceRevisionDate);
			source.setRevisionDate(sourceDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return source;
	}
	
	private List<Map<String, String>> createMapFromFileCSV(CSVReader reader, List<String> schema)  {
		
		List<Map<String, String>> data = new ArrayList<Map<String, String>>();
		
		String[] line = null;
        try {
     
            while ((line = reader.readNext()) != null) {
            	Map<String, String> singleData = new HashMap<String, String>();
            	
		        	for(int i=0; i< line.length; i++) {
		        		String key = schema.get(i).toString().trim();
		        		String value = line[i].toString().trim();
		        		singleData.put(key, value);
		        	}
		        	data.add(singleData);
            }
    		
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return data;
        
	}
	

	private void replaceSeparatorCharacterInFieldsCSV(String inFile) {
		
		// replace ';' separator with ','		
		BufferedReader buffReader = null;
		BufferedWriter buffWriter = null;
		
		String outFile = inFile+"2";
		File inFileObj = null;
		try {
			inFileObj = new File(inFile);
			
			buffReader = new BufferedReader(new FileReader(inFileObj));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		try {
			File outFileObj = new File(outFile);
			buffWriter = new BufferedWriter(new FileWriter(outFileObj));
			String line;
			while((line = buffReader.readLine()) != null) {
				line = line.replaceAll("\";\"", "☺");
				line = line.replaceAll(";", ",");
				line = line.replaceAll("☺", "\";\"");
				buffWriter.write(line);
				buffWriter.newLine();
			}
			buffWriter.close();
			buffReader.close();
			
			inFileObj.delete();
			outFileObj.renameTo(inFileObj);

		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	private Person setPersonFields(Map<String,String> data) {
		
		// save person information into Person object
		Person person = new Person();
		
		String personFirst = data.get(peopleSchema.get(1));
		if(personFirst != null && !personFirst.equals("")) {
			person.setFirstName(personFirst);
		}
		
		String personLastName = data.get(peopleSchema.get(0));
		if(personLastName != null && !personLastName.equals("")) {
			person.setLastName(personLastName);
		}
				
		String personInstitutionName = data.get(peopleSchema.get(3)).trim(); // Institution Name
		
		String ID = data.get(peopleSchema.get(5));
		if( ID == null ) {
			ID = data.get("Person ID SNSF");
		}
		
		if(personInstitutionName != null && !personInstitutionName.equals("")) {
			if(peopleOrgsData.containsKey(ID) && peopleOrgsData.get(ID) != null )
				personInstitutionName += "☺"+peopleOrgsData.get(ID); //remember that some people may be associated to multiple institutions
		}
		
		if( personInstitutionName != null && !personInstitutionName.equals("")){
			if( ID != null ) {
				peopleOrgsData.put(ID, personInstitutionName);
			}
		}
		return person;
	}
	
	private List<PersonExtraField> setPersonExtraFields(Map<String,String> data, Set<String> attributes, Person prs) {
		
		// save person extra information into PersonExtraField objects
		List<PersonExtraField> prsExtraFields = new ArrayList<PersonExtraField>();
		
		for (String key : data.keySet()) {
			if (attributes.contains(key)) {
				if (!data.get(key).equals("")) {
					PersonExtraField prsExtraField = new PersonExtraField();
					prsExtraField.setFieldKey(key);
					prsExtraField.setFieldValue(data.get(key));
					prsExtraField.setPerson(prs);
					
					// check field value length
					if(prsExtraField.getFieldValue().length() > 255) prsExtraField.setFieldValue(null);
					
					prsExtraFields.add(prsExtraField);
				}
			}
		}
		
		return prsExtraFields;
	}
	
	private List<PersonIdentifier> setPersonIdentifiers(Map<String,String> data, Person person){
		
		// save person identifier information into PersonIdentifier objects
		List<PersonIdentifier> perIds = new ArrayList<PersonIdentifier>();

		PersonIdentifier perId = new PersonIdentifier();
		String per = data.get(peopleSchema.get(5));
		if(per == null || per.equals("")) {
			per = data.get("Person ID SNSF");
		}
		if(per != null && !per.equals(""))
		{
			perId.setIdentifier(per);
			perId.setProvenance(sourceName);
			perId.setIdentifierName(peopleSchema.get(5));
			perId.setPerson(person);
			
			perIds.add(perId);
		}
		
		PersonIdentifier perId1 = new PersonIdentifier();
		String orcidId = data.get("ORCID");
		if(orcidId == null || orcidId.equals("")) {
			perId1.setIdentifier(orcidId);
			perId1.setProvenance(sourceName);
			perId1.setIdentifierName("ORCID id");
			perId1.setPerson(person);
			
			perIds.add(perId1);
		}
		
		return perIds;
	}
	
	private Map<String, Person> importPerson(EntityManager entitymanager, List<Source> sources, boolean incrementalMode, boolean debugMode){
		
		// import person data into the database
		this.peopleData = createMapFromFileCSV(peopleReader,peopleSchema);
				
		System.out.println("Starting to import people...");
		Map<String,Person> personMap = new HashMap<>();		
		
		Set<String> attributes = new HashSet<String>(Arrays.asList("Gender")); 
        
        boolean start = true;
        
        // read CSV file line by line
	    for(int j=0; j<peopleData.size(); j++) {
	        if ((j % 10000) == 0) {
	        		System.out.println(j);
	        	}
	        
	        // CSV data row
	    	Map<String, String> rowData = peopleData.get(j);
	    	
			if (incrementalMode == true && start) {
	            Query query_ppl = entitymanager.createQuery("Select p FROM PersonIdentifier p WHERE p.identifier_name=:idName and p.identifier_value=:id");
	            query_ppl.setParameter("idName", peopleSchema.get(5));
	            query_ppl.setParameter("id", rowData.get(peopleSchema.get(5)));    		
	            List<PersonIdentifier> ppl = query_ppl.getResultList();
	            
	            if (ppl.size() > 0) {
	            	continue;          
	            } else {
	            	if (start == true) {
	            		System.out.println("Starting from index: " + j);
	            		start = false;
	            	}
	            }
	    	}	    	
			
	    	// set person fields (main fields).
			Person person = setPersonFields(rowData);
			
	      	// set person identifiers
	    	List<PersonIdentifier> personIdentifiers = setPersonIdentifiers(rowData, person);

	    	// create PersonIdentifier tuples in the DB
	    	for (PersonIdentifier personIdentifier: personIdentifiers) {
	    		entitymanager.persist(personIdentifier);
	    	}
	    	// index person with 'Person ID SNSF' code
	    	String personId = rowData.get(peopleSchema.get(5));
	        if (personId != null && !personId.equals("")) {
	        	personMap.put(personId, person);
	        }
	    		        
	    	// connect the source to the related person
	    	if (person != null) {
	    		person.setSources(sources);
	    	}
	    	
	    	// set person extra fields
        	if (person != null) {
        		List<PersonExtraField> prsExtraFields = setPersonExtraFields(rowData, attributes, person);
        		for (PersonExtraField prsExtraField: prsExtraFields) {
        			entitymanager.persist(prsExtraField);
        		}
        	}
	    	
	    	if (person != null) {
	    		entitymanager.persist(person);
	    	}        	
	    	
	    	
	    }
	    
	    System.out.println("Committing " + peopleData.size() + " people...");	    
	    
	    System.out.println("Imported " + peopleData.size() + " people.");
		return personMap;
	}	
	
	private Publication setPublicationFields(Map<String,String> data) {
		
		// save publication information into Publication object		
		Publication publication = new Publication();
		
		String publicationTitle = data.get(publicationSchema.get(4));	// Title of publication
		if(publicationTitle != null && !publicationTitle.equals("")){
			publication.setTitle(publicationTitle);
		}
		
		String publicationYear = data.get(publicationSchema.get(7)); 	// Publication Year
		if(publicationYear != null && !publicationYear.equals("")){
			Calendar cal = Calendar.getInstance();
			
			cal.set(Calendar.YEAR, Integer.parseInt(publicationYear));
			cal.set(Calendar.MONTH, 1);
			cal.set(Calendar.DAY_OF_MONTH, 1);
			
			publication.setPublicationDate(cal.getTime());
		}
		
		String publicationDesc = data.get(publicationSchema.get(25));	// Abstract
		if(publicationDesc != null && !publicationDesc.equals("")){
			publication.setDescription(publicationDesc);
		}
		
		String pubURL = data.get(publicationSchema.get(14));	// Open Access URL
		if(pubURL != null && !pubURL.equals("")) {
			publication.setUrl(pubURL);
		}
		
		String pubType = data.get(publicationSchema.get(3)); // Type of Publication
		if(pubType != null && !pubType.equals("")) {
			publication.setType(pubType);
		}
		
		String pubAuthors = data.get(publicationSchema.get(5)); // Authors
		if(pubAuthors != null && !pubAuthors.equals("")) {
			Map<String, String> authorsMap = new HashMap<String, String>();
			authorsMap.put(data.get(publicationSchema.get(0)), pubAuthors);
		}
		
		return publication;
	}
	
	private List<PublicationIdentifier> setPublicationIdentifiers(Map<String,String> data, Publication publication) {

		// save publication identifier into PublicationIdentifier objects
		List<PublicationIdentifier> pubIds = new ArrayList<PublicationIdentifier>();

		PublicationIdentifier pubId = new PublicationIdentifier();
		String pub = data.get(publicationSchema.get(0));	// Publication ID SNSF
		if(pub != null && !pub.equals(""))
		{
			pubId.setIdentifier(pub);
			pubId.setProvenance(sourceName);
			pubId.setIdentifierName(publicationSchema.get(0));
			pubId.setPublication(publication);
			
			pubIds.add(pubId);
		}
		
		// the connection between publications and projects is performed via Project Number 
		pubId = new PublicationIdentifier();
		pub = data.get(publicationSchema.get(1)); // Project Number
		if(pub != null && !pub.equals(""))
		{
			pubId.setIdentifier(pub);
			pubId.setProvenance(sourceName);
			pubId.setIdentifierName(publicationSchema.get(1));
			pubId.setPublication(publication);
			pubId.setVisibility(false);
			pubIds.add(pubId);
		}
		
		pubId = new PublicationIdentifier();
		pub = data.get(publicationSchema.get(8)); // ISBN
		if(pub != null && !pub.equals(""))
		{
			pubId.setIdentifier(pub);
			pubId.setProvenance(sourceName);
			pubId.setIdentifierName(publicationSchema.get(8));
			pubId.setPublication(publication);
			
			pubIds.add(pubId);
		}
		
		pubId = new PublicationIdentifier();
		pub = data.get(publicationSchema.get(9)); // DOI
		if(pub != null && !pub.equals(""))
		{
			pubId.setIdentifier(pub);
			pubId.setProvenance(sourceName);
			pubId.setIdentifierName(publicationSchema.get(9));
			pubId.setPublication(publication);
			
			pubIds.add(pubId);
		}
		
		return pubIds;

	}
	

	/*private List<PublicationExtraField> setPublicationExtraFields(Map<String,String> data, Set<String> attributes, Project project) {
		
		List<PublicationExtraField> pubExtraFields = new ArrayList<PublicationExtraField>();
		
		
		for (String key : data.keySet()) {
			if (attributes.contains(key)) {
				if (!data.get(key).equals("")) {
					PublicationExtraField publicationExtraField = new PublicationExtraField();
					publicationExtraField.setFieldKey(key);
					publicationExtraField.setFieldValue(data.get(key));
					publicationExtraField.setProject(project);
					pubExtraFields.add(publicationExtraField);
				}
			}
		}
		
		return pubExtraFields;
	}*/

	
	public Map<String, Publication> importPublications(EntityManager entitymanager, List<Source> sources, boolean incrementalMode, boolean debugMode) {
		
		// read publication data and import it into the database
		this.publicationData = createMapFromFileCSV(publicationReader, publicationSchema);
		 
		System.out.println("Starting to import publications...");
        Map<String,Publication> publicationMap = new HashMap<String,Publication>(); 
        Set<String> attributes = new HashSet<String>(Arrays.asList("Authors", "Book Title", "Publisher", "Editors", "Journal Title", "Proceeding Title", "Proceeding Place"));        
        boolean start = true;
        
        // read CSV file line by line
        for(int i=0; i<publicationData.size(); i++) {
        	if ((i % 10000) == 0) {
        		System.out.println(i);
        	}
        	
        	// CSV data row
        	Map<String, String> rowData = publicationData.get(i);
            
        	// set main publication fields
        	Publication pub = setPublicationFields(rowData);
        	
        	
        	// set publication identifiers
        	if (pub != null) {
        		List<PublicationIdentifier> pubIdentifiers = setPublicationIdentifiers(rowData, pub);
        		pub.setPublicationIdentifiers(pubIdentifiers);
        		// create PublicationIdentifier tuples in the DB
        		for (PublicationIdentifier pubIdentifier: pubIdentifiers) {
        			entitymanager.persist(pubIdentifier);
        		}        		
        	}
        	
        	// connect the source to the related organization
        	if (pub!= null) {
        		pub.setSources(sources);
        	}
        	
        	String pubId = rowData.get(publicationSchema.get(0));
        	if (!pubId.equals("")) {
        		publicationMap.put(pubId, pub);
        	}
        	if (pub != null) {
        		entitymanager.persist(pub);
        	}
        	
        }
        System.out.println("Committing " + publicationMap.size() + " publications...");
        System.out.println("Imported " + publicationMap.size() + " publications.");

        return publicationMap;
	}
	
	private Organization setOrganizationFields(Map<String,String> data) {
		
		// save organization information into Organization object
		Organization org = null;
		
		String country = data.get("Institution Country");
		ArrayList<String> validCountries = new ArrayList<String>(Arrays.asList("Austria", "France", "Germany", "Italy", "Liechtenstein", "Slovenia", "Switzerland"));
		if (!country.equals("") && validCountries.contains(country)) {
			org = new Organization();
			org.setCountry(country);
		}
		
		if (org != null) {		
		
			String label = data.get("Institution");
			if (!label.equals("") && label != null) {
				org.setLabel(label);
			}
			
		    //Only 6 country code allowed:
			if( country.equals("Italy") 		|| 
				country.equals("France") 		|| 
				country.equals("Germany") 		|| 
				country.equals("Switzerland") 	|| 
				country.equals("Slovenia") 		|| 
				country.equals("Austria")		||
				country.equals("Liechtenstein") ) {
				
				if(country.equals("Italy")) org.setCountryCode("IT");
				if(country.equals("France")) org.setCountryCode("FR");
				if(country.equals("Germany")) org.setCountryCode("DE");
				if(country.equals("Switzerland")) org.setCountryCode("CH");
				if(country.equals("Slovenia")) org.setCountryCode("SI");
				if(country.equals("Austria")) org.setCountryCode("AT");
				if(country.equals("Liechtenstein")) org.setCountryCode("LI");
				
			}
		}
	    return org;
		
	}
	
	private void connectProjectToOrganization(EntityManager entitymanager, String prjId, Map<String,Project> projectMap, Organization org) {
		
		// connect projects to organization
		if (prjId != null) {
			Project prj = projectMap.get(prjId);
			if (prj != null) {
				if (org.getProjects() != null) {
					List<Project> oldProjects = org.getProjects();
					oldProjects.add(prj);
					org.setProjects(oldProjects);
				} else {
					List<Project> newProjects = new ArrayList<>();
					newProjects.add(prj);
					org.setProjects(newProjects);
				}
			}
		}
	}
	
	
	private Map<String,Organization> importNewOrganizations(EntityManager entitymanager, List<Source> sources, boolean incrementalMode, Map<String,Project> projectMap) {
		
		// import organization data into the database
		this.orgData = createMapFromFileCSV(organizationReader, projectSchema);		
		System.out.println("Starting to import organizations...");		
	    Map<String, Organization> uniqueOrgMap = new HashMap<String, Organization>();
	     
	    // read CSV file line by line
	    for(int i=0; i<orgData.size(); i++) {
	    	if ((i % 10000) == 0) {
	     		System.out.println(i);
	     	}
	    	
	    	// CSV data row
	    	Map<String, String> rowData = orgData.get(i);
	    	
	    	// set main institute fields
	    	Organization inst = setOrganizationFields(rowData);
	    	if (inst == null || inst.getLabel() == null || inst.getLabel().equals("") ) {
	    		inst = null;
	    	}
	    	
	    	// connect institute with projects
	    	if (inst != null) {
	    		
	        	// set unique identifier
	        	/*String identifier = sourceName + "::" + inst.getLabel();
				OrganizationIdentifier orgId = new OrganizationIdentifier();
				orgId.setIdentifier(identifier);
				orgId.setProvenance(sourceName);
				orgId.setIdentifierName("lid");
				orgId.setOrganization(inst);
				entitymanager.persist(orgId);*/
	    		
	    		// check if institute organization has been already inserted
	        	String orgLabel = inst.getLabel();        	
	        	if (uniqueOrgMap.containsKey(orgLabel)) { // retrieve the institute already inserted
	        		inst = uniqueOrgMap.get(orgLabel);
	        	} else {        		
	        		//instPersist = true;
	        		inst.setSources(sources);
	        		entitymanager.persist(inst);
	        		uniqueOrgMap.put(orgLabel, inst);
	        	}        	        	
	        	
	        	String prjId = rowData.get("Project Number");
	        	connectProjectToOrganization(entitymanager, prjId, projectMap, inst);        	        	
	        	
	        	entitymanager.merge(inst);
	    		
	    	}
	    	
	    	// set main university fields 
	    	Organization uni = null;
			String uniLabel = rowData.get("University");
			if (!uniLabel.equals("") && uniLabel != null && !uniLabel.startsWith("Non-profit") && !uniLabel.startsWith("Unassignable") && !uniLabel.startsWith("Companies")) {
				uni = new Organization();
				uniLabel = uniLabel.replaceAll("\\s+", " ");
				uniLabel = uniLabel.trim();
				uni.setLabel(uniLabel);
				uni.setCountry("Switzerland");
				uni.setCountryCode("CH");						
			}
			
			if(uni == null || uni.getLabel() == null || uni.getLabel().equals("") ) {
				uni = null;
	    	}
	    	
	    	if (uni != null) {
	    		
	    		// set unique identifier
	        	/*String identifier = sourceName + "::" + uni.getLabel();
				OrganizationIdentifier orgId = new OrganizationIdentifier();
				orgId.setIdentifier(identifier);
				orgId.setProvenance(sourceName);
				orgId.setIdentifierName("lid");
				orgId.setOrganization(uni);
				entitymanager.persist(orgId);*/
	    		    		
	    		// check if university organization has been already inserted
	        	String univLabel = uni.getLabel();
	        	if (uniqueOrgMap.containsKey(univLabel)) { // retrieve the university already inserted
	        		uni = uniqueOrgMap.get(univLabel);
	        	} else {      		
	        		uni.setSources(sources);
	        		entitymanager.persist(uni);
	        		uniqueOrgMap.put(univLabel, uni);
	        	}
	        	
	        	// connect the university with the projects if the institute doesn't exist
	        	if (inst == null && uni != null) {
	        		String prjId = rowData.get("Project Number");
	        		connectProjectToOrganization(entitymanager, prjId, projectMap, uni);
	        	}
	        	
	        	// if the university is from Switzerland, connect it with institute
	        	if (inst != null && inst.getCountry().equals("Switzerland") && uni != null) {
	        		List<Organization> cOrgs = uni.getChildrenOrganizations();
	        		if (cOrgs != null) {
	        			boolean insert = true;
	        			for (Organization cOrg: cOrgs) {
	        				if (cOrg.getLabel().equals(inst.getLabel())) {
	        					insert = false;
	        					break;
	        				}
	        			}
	        			if (insert) {
	        				cOrgs.add(inst);
	        				uni.setChildrenOrganizations(cOrgs);
	        			}
	        		} else {
	        			cOrgs = new ArrayList<>();
	        			cOrgs.add(inst);
	        			uni.setChildrenOrganizations(cOrgs);
	        		}	        		
	        	}	    		
	    	}
			
	    	if (uni != null ) {
	    		entitymanager.merge(uni);
	    	}	    		
	    }
	    
	    System.out.println("Committing " + uniqueOrgMap.size() + " organizations");	    	    
	    System.out.println("Imported " + uniqueOrgMap.size() + " organizations");
	    
	    return uniqueOrgMap;
	}
	

	private Project setProjectFields(Map<String,String> data,boolean hasAbstract) {
	 	 
		// save project information into Project object
		Project project = new Project();
		
    	String label = data.get("Project Title");
		if (!label.equals("")) {
			project.setLabel(label);
		}
	    if(hasAbstract) {
		    String descr = data.get("Abstract"); //'Lay summary' and 'Lay summary - Lead' are also good alternatives
		    if (!descr.equals("")) {
		    	project.setDescription(descr);
		    }
	    }
	    Calendar cal = Calendar.getInstance();
	    String startDateString = data.get("Start Date");
	    if (!startDateString.equals("")) {
	    	
	    	DateFormat df = new SimpleDateFormat("dd.MM.yyyy");
	    	
			try {
				Date startDate = df.parse(startDateString);
				cal.setTime(startDate);
				project.setStartDate(startDate);
				project.setYear(Integer.toString(cal.get(Calendar.YEAR)));
				project.setMonth(Integer.toString(cal.get(Calendar.MONTH)));
			} catch (ParseException e) {
				e.printStackTrace();
			}
	    }
	    	    
	    int monthDuration = 0;
	    String endDateString = data.get("End Date");
	    if (!endDateString.equals("")) {
	    	
	    	DateFormat df = new SimpleDateFormat("dd.MM.yyyy");
	        
			try {
				Date endDate = df.parse(endDateString);
				Calendar cal2 = Calendar.getInstance();
				cal2.setTime(endDate);
			    monthDuration = ((cal2.get(Calendar.YEAR)-cal.get(Calendar.YEAR))*12)
										+(cal2.get(Calendar.MONTH)-cal.get(Calendar.MONTH));
			} catch (ParseException e) {
				e.printStackTrace();
			}
	    }
	    
	    if ( monthDuration >= 0 ) {
	    	int dayDuration = monthDuration;
	    	String dayDurationString = Integer.toString(dayDuration);
	    	project.setDuration(dayDurationString);
	    }
	    
	    String budget = data.get(projectSchema.get(16)); // Approved Amount
	    if (!budget.equals("") && !budget.equals("data not included in P3")) {
	    	project.setBudget(budget);
	    }

	    return project;
	}		
	
	private List<ProjectIdentifier> setProjectIdentifiers(Map<String,String> data, Project project) {
		
		// save project identifier information into ProjectIdentifier objects
		List<ProjectIdentifier> projectIds = new ArrayList<ProjectIdentifier>();
	  		
		ProjectIdentifier projectId = null;
		
	  	String projectIdentifier = data.get(projectSchema.get(1)); // Project Number String
	  	if (!projectIdentifier.equals("")) {
	  		projectId = new ProjectIdentifier();
	  		projectId.setIdentifier(projectIdentifier);
	  		projectId.setProvenance(sourceName);
	  		projectId.setIdentifierName(projectSchema.get(1));
	  		projectId.setProject(project);
	  		projectIds.add(projectId);
	  	}
	  	
	  	
	  	return projectIds;
	  	
	}
	
	private List<Thematic> setProjectThematics(EntityManager entitymanager, Map<String,String> data, Project project, Map<String,Thematic> thematicMap) {
		
		// save project thematic information into Thematic objects
		List<Thematic> thematics = new ArrayList<>();
		
		Thematic thematic = new Thematic();
	    thematic.setLabel(data.get(projectSchema.get(11)).trim()); // Discipline Name
	    thematic.setCode(data.get(projectSchema.get(10)).trim());  // Discipline Number
	    
	    if( !thematic.getCode().equals("") && !thematic.getLabel().equals("") ) {
	    	
	    	if (thematicMap.containsKey(thematic.getCode())) {
	    		thematic = thematicMap.get(thematic.getCode());
	    	} else {
	    		entitymanager.persist(thematic);
	    		thematicMap.put(thematic.getCode(), thematic);
	    	}
	    	boolean insert = true;
	    	for (Thematic t: thematics) {
	    		if (t.getCode().equals(thematic.getCode()) && t.getLabel().equals(thematic.getLabel())) {
	    			insert = false;
	    			break;
	    		}
	    	}
	    	
	    	if(insert) {
	    		thematics.add(thematic);
	    	}
	    }
	    	
	    return thematics;	
	    
	}
	
	private Map<String,Project> importProjects(EntityManager entitymanager, List<Source> sources, boolean incrementalMode, boolean hasAbstract,boolean debugMode) {
		
		// read project data and import it into the database
		this.projectData = createMapFromFileCSV(projectReader, projectSchema);		 
		System.out.println("Starting to import projects...");
		Map<String,Project> projectMap = new HashMap<String,Project>(); 
		Set<String> attributes = new HashSet<String>(Arrays.asList(/*Used as IDs*/"Project Number","Project Number String"/**/,
																   /*Field*/"Project Title", /*"Project Title English", <- extraField*/
																   "Responsible Applicant", "Funding Instrument",
																   "Funding Instrument Hierarchy", "Institution", 
																   "Institution Country", "University", /*Fields*/"Discipline Number",
																   "Discipline Name", "Discipline Name Hierarchy"/**/,
																   "All disciplines", /*Fields*/"Start date", "End date"/**/, 
																   /*Field*/"Approved Amount", "Keywords", 
																   /* Following attributes are only present in extended file (hasAbstract) */
																   /*Field*/"Abstract",
																   /*"Lay Summary Lead (English)", "Lay Summary (English)", <- extraFields*/
																   "Lay Summary Lead (German)", "Lay Summary (German)", 
																   "Lay Summary Lead (French)", "Lay Summary (French)",
																   "Lay Summary Lead (Italian)", "Lay Summary (Italian)"));
			
		Map<String, Thematic> thematicMap = new HashMap<>();      
		boolean start = true;
		
		// read CSV file line by line
        for(int j=0; j<projectData.size(); j++) {
	        if ((j % 10000) == 0) {
	        	System.out.println(j);
	        }
	        
	        // CSV data row
        	Map<String, String> rowData = projectData.get(j);

        	if (incrementalMode == true && start) {
	            Query query_prjs = entitymanager.createQuery("Select p FROM ProjectIdentifier p WHERE p.identifierName=:idName and p.identifier=:id");
	            query_prjs.setParameter("idName", projectSchema.get(0));
	            query_prjs.setParameter("id", rowData.get(projectSchema.get(0)));    		
	            List<ProjectIdentifier> prjs = query_prjs.getResultList();
	            
	            if (prjs.size() > 0) {
	            	continue;          
	            } else {
	            	if (start == true) {
	            		System.out.println("Starting from index: " + j);
	            		start = false;
	            	}
	            }
        	}
        	
        	// set main project fields
        	Project project = setProjectFields(rowData, hasAbstract);

        	if (project != null) {
        		
        		
        		List<ProjectIdentifier> projectIdentifiers = setProjectIdentifiers(rowData, project);        		        		
        		project.setProjectIdentifiers(projectIdentifiers);
        		
        		// index project with 'Project Number' code
        		String projectId = rowData.get(projectSchema.get(0));
            	if (!projectId.equals("")) {
            		projectMap.put(projectId, project);
            	}
        	}
        	
        	// connect the source to the related project
        	if (project != null) {
        		project.setSources(sources);
        	}
            
        	List<Thematic> thematics = setProjectThematics(entitymanager, rowData, project, thematicMap);
        	if (thematics != null && thematics.size() > 0) {
        		project.setThematics(thematics);
        	}
        	if (project != null) {
        		entitymanager.persist(project);
        	}       	
        	if(debugMode) {
	        	try {
					System.out.println("Press a key to continue...");
					System.in.read(new byte[2]);
				} catch (IOException e) {
					e.printStackTrace();
				}
        	}
        	
        	
        }
        System.out.println("Committing " + projectMap.size() + " projects...");        
        System.out.println("Imported " + projectMap.size() + " projects.");
        
        return projectMap;
		
	}
	
	private Map<String,Publication> linkProjectPublication(EntityManager entitymanager, Map<String,Project> projectMap, Map<String,Publication> publicationMap, boolean incrementalMode, boolean debugMode) {

		// connect publications with project
		System.out.println("Linking each project with its publications...");
		
		if(debugMode) {
			try {
				
				int count = 0;
				System.out.println("Starting linkProjectPublication...");
				System.in.read(new byte[2]);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		int linksNumber = 0;
		if(debugMode)
			System.out.println("Link Publications to their Project : [Publication Key]  [Project Key]");
		
		Set<String> publicationSet = publicationMap.keySet();
		for(String key : publicationSet) {
			
			if(linksNumber % 1000 == 0)
				System.out.println(linksNumber);

			if(publicationMap.get(key) != null) {
				Publication publication = publicationMap.get(key);
				
				String currentProjectIdentifier = (publication.getPublicationIdentifiers().get(1).getIdentifierName().equals("Project Number")) //is Project Number
						?publication.getPublicationIdentifiers().get(1).getIdentifier()
						:publication.getPublicationIdentifiers().get(0).getIdentifier();
				
				Project project = projectMap.get(currentProjectIdentifier);
				
				if(project != null) {
					List<Project> fooPrjList = new ArrayList<Project>();
					fooPrjList.add(project);
					publication.setProjects(fooPrjList);
				}
				
				if(publication != null && project != null)
					publicationMap.put(key, publication);
			
				if(debugMode) {
					try {
					System.out.println("\t\t-> "+key+" "+publicationMap.get(key).getProjects().get(0).getProjectIdentifiers().get(0).getIdentifier());
					
						System.out.println("Continue...");
					} catch (NullPointerException nullEx) {
						System.out.println("Null pointer: "+publicationMap.get(key).getPublicationIdentifiers().get(0).getIdentifier()+" "+ publicationMap.get(key).getPublicationIdentifiers().get(1).getIdentifier());
					}
				}
				entitymanager.merge(publicationMap.get(key));
				linksNumber++;				
			}
		}
		
		if(debugMode) {
			try {
				System.out.println("Links between Project and its publications has been committed.\nCheck?...");
				System.in.read(new byte[2]);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		System.out.println("Committing "+ linksNumber+" Publication-Project links...");		
		System.out.println(linksNumber+" Publication-Project links built.");
		if(debugMode) {
			try {
				System.out.println("Links between Project and its publications have been committed.\nCheck?...");
				System.in.read(new byte[2]);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return publicationMap;
		
	}

	
	private Map<String, Organization> importNewPeopleOrganization(EntityManager entitymanager, List<Source> sources, Map<String,Organization> orgMap, Map<String, Person> peopleMap,boolean incrementalMode, boolean hasAbstract,boolean debugMode){
		 
		 
		 Person p = null;
		 Organization o = null;
		 long orgCount = 0;
		 long newOrgsCount = 0;
		 long errorPerson = 0;
		 
		 System.out.println("Import people institutions, peopleOrgsData size: "+ peopleOrgsData.size());

		 for(String currIdentifier : peopleOrgsData.keySet()) {
			 if(orgCount % 1000 == 0) 
				 System.out.println(orgCount);
			 
			 p = null;
			 o = null;

			 String []institutions = peopleOrgsData.get(currIdentifier).split("☺");
			 
			 if(institutions == null || institutions.length == 0 || institutions[0] == null || institutions[0].equals(""))
				 continue;
			 //check if existing people 
			 boolean foundPerson = false;
			 for(String personId : peopleMap.keySet()) {
				 if(personId.equals(currIdentifier)) {
					 foundPerson = true;
					 p = peopleMap.get(personId);
					 break;
				 }
			 }
			 if(!foundPerson) {
				 errorPerson++;
				 System.out.println("No person found:\n"+currIdentifier+" - error# "+errorPerson);
				 continue;
			 }
			 
			 // check if existing organization			 
			 for(String inst : institutions) {
				 
				 o = orgMap.get(inst);
				 if (o == null) {
					 continue;
				 }
				 
				 Organization instOrg = orgMap.get(inst);
				 if (instOrg != null) {
					 String city = instOrg.getCity();
					 if (city != null) {
						 o.setCity(city);
					 }
				 }
				 // now p is the person and o the organization
				 // retrieving current organization people, creating a new List if null
				 List<Person> peopleList = o.getPeople();
				 if(peopleList == null)
					 peopleList = new ArrayList<Person>();
				 
				 boolean checkPerson = false;
				 for(Person pers : peopleList) {
					 if( pers != null &&
						 pers.getLastName().equals(p.getLastName()) &&
						 pers.getFirstName().equals(p.getFirstName()))
						 checkPerson = true;
				 }
				 if(!checkPerson)
					 peopleList.add(p);
				 
				 o.setPeople(peopleList);
				 
				 //updating orgMap
				 orgMap.put(inst, o);
				 
				 entitymanager.merge(o);
			 }
			 orgCount+=1;
			 
		 }
			 
		 System.out.println("Committing "+orgCount+" people organizations, orgMap size: "+orgMap.size()+", new organizations: "+newOrgsCount);
		 System.out.println("Committed.");
		 
		 return orgMap;
	 }			 				
	
	public static Source getSourceByName(EntityManager entitymanager, String sourceName) {
		
		// retrieve data source by name from database
		Query query = entitymanager.createQuery("Select s FROM Source s where s.label=:sourceLabel");
		query.setParameter("sourceLabel", sourceName);
		List<Source> sources = query.getResultList();
		Source source = sources.get(0);
		return source;
	}
	
	public static List<Organization> retrieveOrganizations(EntityManager entitymanager, Source source) {

		// retrieve organizations appertaining to a given data source from database
		entitymanager.getTransaction().begin();
		Query query = entitymanager.createQuery("Select o FROM Organization o WHERE :id MEMBER OF o.sources");
		query.setParameter("id", source);
		List<Organization> orgs = query.getResultList();

		entitymanager.getTransaction().commit();
		System.out.println("Retrieved " + orgs.size() + " organizations");

		return orgs;
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
	
	public void importData(String entity, boolean incrementalMode, boolean hasAbstract, boolean debugMode, String db) {
		
		// initialize entity manager factory
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();
						        
        // get or create P3 source ------------------------------------------------
        entitymanager.getTransaction().begin();
        Query query = entitymanager.createQuery("SELECT s FROM Source s WHERE s.label = :label");
        query.setParameter("label", sourceName);
        List<Source> P3Source = query.getResultList();
        Source source = null;
        if (P3Source.size() > 0) {
        	source = P3Source.get(0);
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
    	
    	Map<String,Project> projectMap = new HashMap<String,Project>();
    	Map<String,Publication> publicationMap = new HashMap<String,Publication>();
    	Map<String,Person> peopleMap =  new HashMap<String,Person>();
    	Map<String,Organization> orgMap = new HashMap<String, Organization>();
    	Map<String, String> orgsNoMatchPeopleMap = new HashMap<String, String>();
    	
    	if (entity.equals("all")) { 
    		
    		entitymanager.getTransaction().begin();
    		
            projectMap = importProjects(entitymanager, sources, incrementalMode,hasAbstract,debugMode);
            publicationMap = importPublications(entitymanager, sources, incrementalMode,debugMode);	
            publicationMap = linkProjectPublication(entitymanager, projectMap, publicationMap, incrementalMode, debugMode);            
            peopleMap = importPerson(entitymanager, sources, incrementalMode,debugMode);
            
            orgMap = importNewOrganizations(entitymanager, sources, incrementalMode, projectMap);
            orgMap = importNewPeopleOrganization(entitymanager, sources, orgMap, peopleMap, incrementalMode,hasAbstract,debugMode);
            entitymanager.getTransaction().commit();
		}
    	
    	if (entity.equals("project")) {			
    		projectMap = importProjects(entitymanager, sources, incrementalMode, hasAbstract,debugMode);
		}
    	    	
    	if (entity.equals("people")) {			
    		peopleMap = importPerson(entitymanager, sources, incrementalMode,debugMode);
		}
    	
    	if (entity.equals("publication")) {			
    		publicationMap = importPublications(entitymanager, sources, incrementalMode,debugMode);
		}    	
		
		entitymanager.close();
		emfactory.close(); 
		
		emfactory = Persistence.createEntityManagerFactory(db);
		entitymanager = emfactory.createEntityManager();
		
		Source sourceP3 = getSourceByName(entitymanager, sourceName);
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
        
	}
	
}
