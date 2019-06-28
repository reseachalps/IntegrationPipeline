package it.unimore.alps.integrator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.enums.CSVReaderNullFieldIndicator;

import it.unimore.alps.sources.cordis.CordisOrganization;
import it.unimore.alps.sql.model.Organization;

public class GeoIntegratorOld {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		CommandLine commandLine;
		Option orgFolderOption = Option.builder("orgGeoTagged").hasArg().required(true)
				.desc("The file that contains organization data. ").longOpt("organizationGeoTagged").build();
		//

		Option dbOption = Option.builder("db").hasArg().required(true)
				.desc("The name of the database of the data ingestion.").longOpt("DB").build();

		Options options = new Options();

		options.addOption(orgFolderOption);

		options.addOption(dbOption);

		CommandLineParser parser = new DefaultParser();

		String orgFileGeoTagged = null;

		String db = null;

		boolean header = true;

		try {
			commandLine = parser.parse(options, args);

			if (commandLine.hasOption("orgGeoTagged")) {
				orgFileGeoTagged = commandLine.getOptionValue("orgGeoTagged");
			} else {
				System.out.println("Organization file with geo information not provided. Use the orgFolder option.");
				System.exit(0);
			}
			System.out.println("Org file: " + orgFileGeoTagged);

			if (commandLine.hasOption("db")) {
				db = commandLine.getOptionValue("db");
			} else {
				System.out.println("Destination DB is not provided. Use the db option.");
				System.exit(0);
			}
			System.out.println("db: " + db);

		} catch (org.apache.commons.cli.ParseException e) {

			e.printStackTrace();
		}

		GeoIntegratorOld integrator = new GeoIntegratorOld();
		//Map<Integer, GeoTaggedOrganization> idORG = integrator.readGeoTaggeOrganization(orgFileGeoTagged);
		
		
		/*Map<String, GeoTaggedOrganization> idORG = integrator.readGeoTaggeOrganization(orgFileGeoTagged);
		System.exit(1);

		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();

		List<Organization> orgs = integrator.retrieveOrganizationWithIds(entitymanager, db);

		entitymanager.getTransaction().begin();

		for (Organization org : orgs) {
			//Integer id = org.getId();
			String id = org.getLabel() + "_" + org.getCity() + "_" + org.getAddress(); 
			if (idORG.containsKey(id)) {

				GeoTaggedOrganization geoTag = idORG.get(id);

				String lat = geoTag.getLat();
				String lon = geoTag.getLon();

				if (lat != null && lon != null) {
					if (!lat.equals("null") && !lon.equals("null")) {
						Float latF = null;
						Float lonF = null;
						try {
							latF = Float.parseFloat(lat);
							lonF = Float.parseFloat(lon);
							
							if (latF.floatValue()>180 || latF.floatValue()<-180) {
								latF = 0f;
							}
							
							if (lonF.floatValue()>180 || lonF.floatValue()<-180) {
								lonF = 0f;
							}
							
						} catch (Exception e) {
							e.printStackTrace();
							System.err.println("Lat: "+lat +"\tLon: "+lon);
							continue;
						}

						System.out.println("ORGANIZATION: " + org.getLabel());
						System.out.println("Lat: " + latF + "\tLon: " + lonF);

						org.setLat(latF);
						org.setLon(lonF);
						entitymanager.merge(org);
					}
				}

			}
		}
		entitymanager.getTransaction().commit();
		entitymanager.close();
		emfactory.close();*/
		
		int insertedGeoCoordinates = 0;
		
		Map<String, Map<String, String>> idORG = integrator.readOrganizationsWithGeoCoordinates(orgFileGeoTagged);
		/*for (String orgKey: idORG.keySet()) {
			System.out.println(orgKey);
			System.out.println(idORG.get(orgKey));
		}
		System.exit(1);*/
		
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();

		List<Organization> orgs = integrator.retrieveOrganizationWithIds(entitymanager, db);

		entitymanager.getTransaction().begin();

		for (Organization org : orgs) {			
			//Integer id = org.getId();
			if (org.getLabel() != null) {
				String id = cleanString(org.getLabel()) + "_" + cleanString(org.getCity()) + "_" + cleanString(org.getAddress()); 
	
				//if (org.getSources().get(0).getLabel().equals("CercaUniversita")) {
					
				System.out.println("Id: " + id);
				//}
				
				if (idORG.containsKey(id)) {
	
					Map<String, String> geoTag = idORG.get(id);
	
					String lat = geoTag.get("lat");
					String lon = geoTag.get("lon");
	
					if (lat != null && lon != null) {
						if (!lat.equals("null") && !lon.equals("null")) {
							Float latF = 0f;
							Float lonF = 0f;
							try {
								latF = Float.parseFloat(lat);
								lonF = Float.parseFloat(lon);
								
								if (latF.floatValue()>180 || latF.floatValue()<-180) {
									latF = 0f;
								}
								
								if (lonF.floatValue()>180 || lonF.floatValue()<-180) {
									lonF = 0f;
								}
								
							} catch (Exception e) {
								e.printStackTrace();
								System.err.println("Lat: "+lat +"\tLon: "+lon);
								continue;
							}
	
							System.out.println("ORGANIZATION: " + org.getLabel());
							System.out.println("Lat: " + latF + "\tLon: " + lonF);
	
							org.setLat(latF);
							org.setLon(lonF);
							entitymanager.merge(org);
							insertedGeoCoordinates+=1;
						}
					}
					
					//if (org.getSources().get(0).getLabel().equals("CercaUniversita")) {
						
					/*System.out.println("TROVATO");
					Map<String, String> geoTag = idORG.get(id);
					System.out.print(cleanString(geoTag.get("label")));
					System.out.print(" ---> ");
					System.out.println(cleanString(org.getLabel()));
					insertedGeoCoordinates+=1;*/
					//}
					
	
				} /*else {
					//if (org.getSources().get(0).getLabel().equals("CercaUniversita")) {
					System.out.println("NON TROVATO");
					//}				
				}*/
			}
		}
		entitymanager.getTransaction().commit();
		entitymanager.close();
		emfactory.close();
		
		System.out.println("Num. geo coordinates inserted: " + insertedGeoCoordinates);

	}

	public List<Organization> retrieveOrganizationWithIds(EntityManager em, String database) {

		em.getTransaction().begin();

		Query query = em.createQuery("Select o FROM Organization o");

		List<Organization> organizations = query.getResultList();

		System.out.println("Number of organizations from database: " + organizations.size());

		em.getTransaction().commit();
		return organizations;

	}
	
	public static String cleanString(String s)	{
		if (s != null) {
		    s = Normalizer.normalize(s, Normalizer.Form.NFD);
		    s = s.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "");
		    s = s.replace("'", "");
		    s = s.toLowerCase();
		    s = s.replace(",", "");
		} else {
			s = "null";
		}
	    return s;
	}
	
	private CSVReader initializeCSVReader(String inputFile, boolean header, List<String> schema) {
		
		CSVParser parser = null;
		CSVReader reader = null;
		
		parser = new CSVParserBuilder().withSeparator('£').withIgnoreQuotations(true).build();
		try {
			reader = new CSVReaderBuilder(new FileReader(inputFile)).withCSVParser(parser).build();
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		// extract the header from input file
		if (reader != null) {
			if (header == true) {
				String[] line;
				try {
					if ((line = reader.readNext()) != null) {
						for(int i=0; i<line.length; i++) {
							schema.add(line[i]);	
						}
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
		//List<Map<String, String>> fullData = new ArrayList<Map<String, String>>();
        try {
            while ((line = reader.readNext()) != null) {
            	Map<String, String> rowData = new HashMap<String, String>();
	        	for(int i=0; i< line.length; i++) {
	        		String key = schema.get(i);
	        		String value = line[i];
	        		if (!value.equals("")) {
	        			rowData.put(key, value.toString());
	        		} else {
	        			rowData.put(key, null);
	        		}
	        	}
	        	/*for(Object objname: row_data.keySet()) {
	     		   System.out.println(objname);
	     		   System.out.println(row_data.get(objname));
	     		 }*/
	        	fullData.add(rowData);
	        	
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return fullData;
        
	}
	
	public Map<String, Map<String, String>> readOrganizationsWithGeoCoordinates(String path) {
		boolean header = true;
		List<String> schema = new ArrayList<String>();
		CSVReader fileReader = initializeCSVReader(path, header, schema);
		List<Map<String, String>> fileData = createMapFromFileCSV(fileReader, schema);
		Map<String, Map<String, String>> fileMap = new HashMap<>();
		System.out.println("Number of organization: " + fileData.size());
		for(int i=0; i<fileData.size(); i++) {
			Map<String, String> rowData = fileData.get(i);
			String label = rowData.get("label"); 
			if (label != null) {
				String key = cleanString(label) + "_" + cleanString(rowData.get("city")) + "_" + cleanString(rowData.get("address"));
				/*if (cleanString(rowData.get("city")).equals("modena")) {
					System.out.println("CSV: " +  key);
				}*/
				fileMap.put(key, rowData);
			}
		}
		return fileMap;
	}

	public Map<String, GeoTaggedOrganizationOld> readGeoTaggeOrganization(String path) {

		// String s = "Sachin,,M,\"Maths,Science,English\",Need to improve in these
		// subjects.";
		//
		// String ss = "25188,,,FIL.VA SRL,,,VIA PER SCHIANNO 63,VARESE
		// VA,,Italy,IT,21100,,,45.79209674715792,8.836195074124172,,,,,,,0,,http://WWW.FILVA.IT,,,,,,";
		//
		// String regex = ",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)";
		//
		// String[] splitted1 = ss.split(regex, -1);
		// System.out.println(splitted1.length);
		// System.out.println("------------------");
		//
		// try (BufferedReader br = new BufferedReader(new FileReader(path))) {
		// for (String line; (line = br.readLine()) != null;) {
		// String[] splitted = line.split(regex, -1);
		//// if(splitted.length !=31) {
		// System.out.println("" + splitted.length);
		// System.out.println("" + line);
		//// }
		//
		// }
		// // line is not visible here.
		// } catch (FileNotFoundException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		
		
		//Map<Integer, GeoTaggedOrganization> map = new HashMap<>();
		Map<String, GeoTaggedOrganizationOld> map = new HashMap<>();
		try {
			List<GeoTaggedOrganizationOld> beans = new CsvToBeanBuilder(new FileReader(path))
					.withType(GeoTaggedOrganizationOld.class).withSeparator(';').build().parse();

			for (GeoTaggedOrganizationOld org : beans) {
				//Integer id = org.getId();
				//map.put(id, org);
				System.out.println(org.getLabel());
				String key = org.getLabel() + "_" + org.getCity() + "_" + org.getAddress();				
				map.put(key, org);
			}

			System.out.println("Number of organization: " + beans.size());

			// System.out.println("FP7 Number of organizations: " + beans.size());
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return map;

	}

}
