package it.unimore.alps.integrator;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import it.unimore.alps.sql.model.Organization;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class DefinitiveLatLongOld {
	
	private static int numInsertedGeoCoordinates = 0;
	
	public static void main(String[] args) throws JSONException {
		CommandLine commandLine;

		Option DB = Option.builder("DB").hasArg().required(true).desc("DB. ").longOpt("DB").build();

		Options options = new Options();

		options.addOption(DB);

		CommandLineParser parser = new DefaultParser();

		String db = null;
		try {
			commandLine = parser.parse(options, args);

			if (commandLine.hasOption("DB")) {
				db = commandLine.getOptionValue("DB");
			} else {
				System.out.println("Source database is not provided. Use the DB option.");
				System.exit(1);
			}
			System.out.println("Database: " + db);

		} catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager(); 
		
		// retrieve entities
		List<Organization> orgs = retrieveOrganizations(entitymanager);					
		
		getOrganizationGeoCoordinatesStats(orgs);
		
		addGeoCoordinates(orgs, entitymanager);
		
		//getOrganizationGeoCoordinatesStats(orgs);
	}
	
	public static List<Organization> retrieveOrganizations(EntityManager entitymanager) {

		entitymanager.getTransaction().begin();
		Query query_org = entitymanager.createQuery("Select o FROM Organization o");
		List<Organization> orgs = query_org.getResultList();

		entitymanager.getTransaction().commit();
		System.out.println("Retrieved " + orgs.size() + " organizations");

		return orgs;
	}
	
	public static void getOrganizationGeoCoordinatesStats(List<Organization> orgs) {
		int numMissingGeoCoordinates = 0;
		for (Organization org: orgs) {
			if (org.getLat() != 0) {
				numMissingGeoCoordinates += 1;
			}
		}
		System.out.println("Num organizations with missing geo coordinates: " + numMissingGeoCoordinates);
	}
	
	private static void addGeoCoordinates(List<Organization> orgs, EntityManager em) {				
		
		for (Organization org: orgs) {
			
			em.getTransaction().begin();
			
			System.out.println("Current org: " + org.getLabel());
			String city = org.getCity();
			String address = org.getAddress();
			String country = org.getCountry();
			System.out.println("\tCity: " + city);
			System.out.println("\tAddress: " + address);
			System.out.println("\tCountry: " + country + "\n");
			
			if(city != null && !city.isEmpty() && !city.equalsIgnoreCase("null")) {
				
				System.out.println("\t\tValid city");
				
				String[] latlon=request(city, address, country);
				if(!latlon[0].equals("null") && !latlon[1].equals("null")){
					System.out.println("\t\t\tFound correct geo coordinates.");
					org.setLat(Float.parseFloat(latlon[0]));
					org.setLon(Float.parseFloat(latlon[1]));
					em.merge(org);
					numInsertedGeoCoordinates += 1;
				}else {
					System.out.println("\t\tNo geo coordinates found.");
				}
				System.out.println("---------------------------");
			}else {
				System.out.println("\t\tInvalid city");
			}
			
			em.getTransaction().commit();
		}
				
		System.out.println("Num. geo coordinates inserted: " + numInsertedGeoCoordinates);
	}
	

	public static String[] request(String city, String address, String country) {
		 String ret[]= {"null","null"};
	     JSONObject Jobject;
	     JSONArray arrayLoc;
		 //controlli per evitare che nella richiesta mandi cose tipo null
		 if(city==null || city.equalsIgnoreCase("null")||city.isEmpty()||city.equals("")) 
		 {
			 city="";
		 }else {
			 city=city.replaceAll(" ", "%20");
		 }
		 if(address==null || address.equalsIgnoreCase("null")||address.isEmpty()||address.equals("")) 
		 {
			 address="";
		 }else {
			 address=address.replaceAll(" ", "%20");
		 }
		 if(country==null || country.equalsIgnoreCase("null")||country.isEmpty()||country.equals("")) 
		 {
			 country="";
		 }else {
			 country=country.replaceAll(" ", "%20");
		 }
		     OkHttpClient client = new OkHttpClient();

		     Request request = new Request.Builder()
		       .url("https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates?f=json&singleLine="+address+"%20"+city+"%20"+country+"&outFields=Match_addr%2CAddr_type")
		       .get()
		       .addHeader("cache-control", "no-cache")
		       .addHeader("postman-token", "25c9935b-d60f-f19f-9134-8aa035af2181")
		       .build();
		     try {
		    	 Response response = client.newCall(request).execute();
			     String jsonData = response.body().string();
				try {
					Jobject = new JSONObject(jsonData);
					arrayLoc = Jobject.getJSONArray("candidates");
				    String Y = arrayLoc.toString();
				    String X = arrayLoc.toString();
				    Y=StringUtils.substringBetween(Y, "\"y\":", "},\"");
				    X=StringUtils.substringBetween(X, "\"x\":", ",\"y\"");
					ret[0]=Y;
					ret[1]=X;
					if(Y==null) {
						ret[0]="null";
					}
					if(X==null) {
						ret[1]="null";
					}
					//System.out.println("\n"+conta);
					System.out.println("\n"+Y+"-------"+X);
				} catch (JSONException e) {
					e.printStackTrace();
				}
		     }catch(IOException e) {
		    	 e.printStackTrace();
		     }     
		return ret;
	}
}

