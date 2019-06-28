package it.unimore.alps.sources.openaire;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

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

import com.opencsv.CSVReader;

import it.unimore.alps.sources.bvd.BvDImporter;
import it.unimore.alps.sql.model.Source;

public class OpenAireDataIntegrator {
	
	private Reader reader;

	public static void main(String[] args) {
		
		CommandLine commandLine;
        Option orgExtraFileOption = Option.builder("orgExtraFile")
        		.hasArg()
        		.required(true)
        		.desc("The file that contains extra data about openaire organizations. ")
        		.longOpt("orgExtraFile")
        		.build();

        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addOption(orgExtraFileOption);
        
        String orgExtraFile = null;
                	
        try {
			commandLine = parser.parse(options, args);
		
			System.out.println("----------------------------");
			System.out.println("OPTIONS:");
			
	        if (commandLine.hasOption("orgExtraFile")) {   
	        	orgExtraFile = commandLine.getOptionValue("orgExtraFile");
	        	System.out.println("\tExtra data organizations file: " + orgExtraFile);
			} else {
				System.out.println("\tExtra data organizations file not provided. Use the orgExtraFile option.");
	        	System.exit(1);
			}					
			
			System.out.println("----------------------------\n");
        
        } catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
        
        // -orgExtraFile /home/matteo/Scrivania/coreex-it.json 
        
        System.out.println("Starting integrating extra OpenAire data...");
        OpenAireDataIntegrator openaireIntegrator = new OpenAireDataIntegrator(orgExtraFile);
        openaireIntegrator.integrateData();

	}
	

	
	public OpenAireDataIntegrator(String orgExtraFile) {
		
		try {
			this.reader = new FileReader(orgExtraFile);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void integrateData() {
		
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory("alps");
		EntityManager entitymanager = emfactory.createEntityManager();
		
		// create openaire source object ---------------
		String sourceLabel = "OpenAire";
		String urlSource = "https://www.openaire.eu/";
		
		String sourceRevisionDate = "2017-12-15";
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

		Date sourceDate = null;
		try {
			sourceDate = df.parse(sourceRevisionDate);
		} catch (java.text.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Source source = new Source();
		source.setLabel(sourceLabel);
		source.setRevisionDate(sourceDate);
		source.setUrl(urlSource);
		// ---------------------------------------------
		
		entitymanager.getTransaction().begin();
		Query queryOrg = entitymanager.createQuery("Select o FROM Organization o WHERE :id MEMBER OF o.sources");
		queryOrg.setParameter("id", source);
		List<Source> sources = queryOrg.getResultList();
        entitymanager.getTransaction().commit();
		    
	    JSONParser parser = new JSONParser();
		
		try {
			
			Object fileObj = parser.parse(reader);
			JSONArray orgsArray = (JSONArray) fileObj;
			Iterator<JSONObject> it = orgsArray.iterator();
			while (it.hasNext()) {
				//Object orgData = parser.parse(it.next());
				//System.out.println(it.next());
				JSONObject orgData = it.next();
				String id = (String) orgData.get("id");
				System.out.println("Id = " + id);
				
				/*JSONArray addresses = (JSONArray) orgData.get("addresses");
				Iterator<JSONObject> addressIterator = addresses.iterator();
				while (addressIterator.hasNext()) {
					JSONObject addressJSONObject = addressIterator.next();
					
					String city = (String) addressJSONObject.get("city");
					System.out.println("\tCity = " + city);
					String address = (String) addressJSONObject.get("address");
					System.out.println("\tAddress = " + address);
					String zipcode = (String) addressJSONObject.get("zipcode");
					System.out.println("\tZipcode = " + zipcode);
				}*/
				
			}

			reader.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
									
		
	}

}
