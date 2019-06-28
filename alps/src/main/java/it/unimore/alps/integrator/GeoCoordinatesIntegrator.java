package it.unimore.alps.integrator;

import java.io.BufferedReader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.opencsv.CSVReader;

import it.unimore.alps.sql.model.GeoCoordinate;
import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationExtraField;
import it.unimore.alps.sql.model.Source;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class GeoCoordinatesIntegrator {
	
	private static int numNewGeoCoordinates = 0;

	public static void main(String[] args) throws JSONException {
		
		// read parameter for the db

		CommandLine commandLine;

		Option DB = Option.builder("DB").hasArg().required(true).desc("DB. ").longOpt("DB").build();
		Option LatLonDB = Option.builder("LatLonDB").hasArg().required(true).desc("LatLonDB.").longOpt("LatLonDB").build();

		Options options = new Options();

		options.addOption(DB);
		options.addOption(LatLonDB);

		CommandLineParser parser = new DefaultParser();

		String db = null;
		String latlogDB = null;
		try {
			commandLine = parser.parse(options, args);

			if (commandLine.hasOption("DB")) {
				db = commandLine.getOptionValue("DB");
			} else {
				System.out.println("Source database not provided. Use the DB option.");
				System.exit(1);
			}
			System.out.println("Database: " + db);
			
			if (commandLine.hasOption("LatLonDB")) {
				latlogDB = commandLine.getOptionValue("LatLonDB");
			} else {
				System.out.println("Lat/Lon database not provided. Use the LatLonDB option.");
				System.exit(1);
			}
			System.out.println("Lat/Lon Database: " + latlogDB);

		} catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();
		EntityManagerFactory latlonEmfactory = Persistence.createEntityManagerFactory(latlogDB);
		EntityManager latlonEntitymanager = latlonEmfactory.createEntityManager();
		
		List<Organization> orgs = retrieveOrganizationsWithoutLatLon(entitymanager);
		Map<String, GeoCoordinate> geoMap = retrieveAlreadyExistingGeoCoordinates(latlonEntitymanager);		
		
		integrateGeoCoordinates(orgs, geoMap, entitymanager, latlonEntitymanager);
		
		System.out.println("Integrated " + numNewGeoCoordinates + " new geo coordinates.");
	}
	
	public static Map<String, GeoCoordinate> retrieveAlreadyExistingGeoCoordinates(EntityManager latlonEntitymanager) {
		
		latlonEntitymanager.getTransaction().begin();
		System.out.println("Starting retrieving already existing geo coordinates...");
		Query queryGeo = latlonEntitymanager.createQuery("Select g FROM GeoCoordinate g");
	    List<GeoCoordinate> geocoordinates = queryGeo.getResultList();
	    latlonEntitymanager.getTransaction().commit();
	    System.out.println("Retrieved " + geocoordinates.size() + " geocoordinates.");
	    	    
		Map<String, GeoCoordinate> geoMap = new HashMap<>();
		
		for (GeoCoordinate geo: geocoordinates) {
			String city = geo.getCity();
		    String country = geo.getCountry();
		    String postcode = geo.getPostcode();
		    String address = geo.getAddress();
		    String geoId = city + "_" + country + "_" + postcode + "_" + address;
		    geoMap.put(geoId, geo);
		}
		
		return geoMap;
		
	}
	
	public static List<Organization> retrieveOrganizationsWithoutLatLon(EntityManager entitymanager) {
			
		entitymanager.getTransaction().begin();
		System.out.println("Starting retrieving organizations without geo coordinates...");
		// USE THIS QUERY IF NO NEW DATA HAS BEEN INSERTED
	    //Query query_org = entitymanager.createQuery("Select o FROM Organization o WHERE (o.lat is null or o.lat=0) and (o.address is not null and o.address <> '') and (o.city is not null and o.city <> '')");
		// USE THIS QUERY IF NEW DATA HAS BEEN INSERTED
		//Query query_org = entitymanager.createQuery("Select o FROM Organization o WHERE (o.address is not null and o.address <> '') and (o.city is not null and o.city <> '')");
		// DEFINITIVE VERSION
		Query query_org = entitymanager.createQuery("Select o FROM Organization o WHERE (o.city is not null or o.city <> '') and (o.lat is null or o.lat = 0)");
		//Query query_org = entitymanager.createQuery("Select o FROM Organization o WHERE o.city is not null or o.city <> ''");
	    List<Organization> orgs = query_org.getResultList();
	    entitymanager.getTransaction().commit();
	    System.out.println("Retrieved " + orgs.size() + " organizations.");
	    
	    return orgs;
	}
	
	public static Float[] retrieveGeoCoordinate(Organization org, Map<String, GeoCoordinate> geoMap, EntityManager latlonEmfactory) {
	
		Float latlon[] = {0f, 0f};
		
		// try to retrieve organization geo coordinate
		//latlonEmfactory.getTransaction().begin();
	    
		/*Query query_latlon = latlonEmfactory.createQuery("Select g FROM GeoCoordinate g WHERE g.city=:city and g.country=:country and g.postcode=:postcode and g.address=:address");
	    query_latlon.setParameter("city", org.getCity());
	    query_latlon.setParameter("country", org.getCountry());
	    query_latlon.setParameter("postcode", org.getPostcode());
	    query_latlon.setParameter("address", org.getAddress());
	    List<GeoCoordinate> geos = query_latlon.getResultList();*/
		
		String city = org.getCity();
	    String country = org.getCountry(); 
	    String postcode = org.getPostcode();
	    String address = org.getAddress();
	    String orgId = city + "_" + country + "_" + postcode + "_" + address;
	    GeoCoordinate geo = geoMap.get(orgId);
	    	        
	    if (geo != null) { // if geo coordinate already exists
	    	//System.out.println("Geo coordinate already existing.");
	    	latlon[0] = geo.getLat();
	    	latlon[1] = geo.getLon();
	
	    } else {
	    	
	    	System.out.println(org.getLabel() + " ------------------------");
			System.out.println("\t" + city);
			System.out.println("\t" + country);
			System.out.println("\t" + postcode);
			System.out.println("\t" + address);
			if (org.getSources() != null && org.getSources().size() > 0) {
				System.out.println("\t" + org.getSources().get(0).getLabel());
			}
			System.out.println("Geo coordinate not existing. Try to retrieve it.");	    	
	    	
	    	String[] stringLatLon = request(city, address, country);
	    	if (!stringLatLon[0].equals("null")) {
		    	latlon[0] = Float.parseFloat(stringLatLon[0]);
		    	latlon[1] = Float.parseFloat(stringLatLon[1]); 
		    	
		    	GeoCoordinate newGeo = new GeoCoordinate();
		    	newGeo.setCity(city);
		    	newGeo.setAddress(address);
		    	newGeo.setCountry(country);
		    	newGeo.setPostcode(postcode);
		    	newGeo.setLat(latlon[0]);
		    	newGeo.setLon(latlon[1]);
		    	latlonEmfactory.persist(newGeo);
		    	
		    	System.out.println("Geo coordinates found: " + latlon[0] + ", " + latlon[1]);
	    	} else {
	    		System.out.println("No geo coordinates found.");
	    		
	    		/*GeoCoordinate newGeo = new GeoCoordinate();
		    	newGeo.setCity(city);
		    	newGeo.setAddress(address);
		    	newGeo.setCountry(country);
		    	newGeo.setPostcode(postcode);
		    	//newGeo.setLat(null);
		    	//newGeo.setLon(latlon[1]);
		    	latlonEmfactory.persist(newGeo);*/
	    	}
	    	
	    	System.out.println("-------------------------------------------");
			System.out.println();
	    }
	    
	    //latlonEmfactory.getTransaction().commit();
	    
	    return latlon;
	    
	}
	
	public static void integrateGeoCoordinate(Organization org, Map<String, GeoCoordinate> geoMap, EntityManager entitymanager, EntityManager latlonEmfactory, boolean fakeGpsData) {
		
		Float[] latlon = retrieveGeoCoordinate(org, geoMap, latlonEmfactory);
		if (latlon != null) {
			Float lat = latlon[0];
	    	Float lng = latlon[1];
	    	org.setLat(lat);
	    	org.setLon(lng);
	    	
	    	List<OrganizationExtraField> oldExtraFields = null;
	    	if (fakeGpsData) {	    		
	    		if (oldExtraFields != null) {
	    			oldExtraFields = org.getOrganizationExtraFields();
	    		} else {
	    			oldExtraFields = new ArrayList<>();
	    		}
	    		OrganizationExtraField oef = new OrganizationExtraField();
	    		oef.setFieldKey("fake gps");
	    		oef.setFieldValue("true");
	    		oef.setVisibility(false);
	    		oef.setOrganization(org);
	    		oldExtraFields.add(oef);
	    		org.setOrganizationExtraFields(oldExtraFields);
	    	}
	    	
	    	entitymanager.merge(org);
	    	numNewGeoCoordinates+=1;
	    	//System.out.println("\tIntegratation complete successfully. " + org.getLabel() + ": lat="+ lat + ", lon=" + lng + ".");
		} else {
			//System.out.println("\tIntegratation not completed.");
		}
		
	}
	
	
	public static void integrateGeoCoordinates(List<Organization> orgs, Map<String, GeoCoordinate> geoMap, EntityManager entitymanager, EntityManager latlonEmfactory) {
		
		entitymanager.getTransaction().begin();
		latlonEmfactory.getTransaction().begin();
		
		int i = 0;
		for (Organization org: orgs) {
			
			if ((i%1000) == 0) {
				System.out.println("Progress: " + i);
			}
			
			i+=1;
			
			boolean fakeGpsData = false;
			if (org.getAddress() == null && org.getCity() != null) {
				fakeGpsData = true;
			}
			
			/*System.out.println("Num: " + i + ", " + org.getLabel() + " ------------------------");
			System.out.println("\t" + org.getCity());
			System.out.println("\t" + org.getCountry());
			System.out.println("\t" + org.getPostcode());
			System.out.println("\t" + org.getAddress());
			if (org.getSources() != null && org.getSources().size() > 0) {
				System.out.println("\t" + org.getSources().get(0).getLabel());
			}*/
			Float lat = org.getLat();
			//if (lat == null || lat == 0) {			
			integrateGeoCoordinate(org, geoMap, entitymanager, latlonEmfactory, fakeGpsData);
			//} else {
			//	System.out.println("\tOrganization with lat/lon");
			//}
			//System.out.println("-------------------------------------------");
			//System.out.println();						
			
		}
		
		entitymanager.getTransaction().commit();
		latlonEmfactory.getTransaction().commit();
		
	}
		
	public static String[] request(String city,String address, String country) {
		 
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
		       //.url("https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates?f=json&singleLine="+address+"%20"+city+"%20"+country+"&outFields=Match_addr%2CAddr_type")
		       .url("https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates?f=json&address="+address+"&city="+city+"&countryCode="+country+"&outFields=Match_addr%2CAddr_type")
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
					//System.out.println("\n"+Y+"-------"+X+"  http request");
				} catch (JSONException e) {
					e.printStackTrace();
				}
		     }catch(IOException e) {
		    	 e.printStackTrace();
		     }     
		return ret;
	}
}