package it.unimore.alps.dedupvalidation;

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

import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.Source;

public class DedupValidator {
	
	public static void main(String[] args) {

		CommandLine commandLine;
		Option preDBOption = Option.builder("preDedupDB").hasArg().required(true).desc("Database pre-deduplication").longOpt("preDedupDB").build();
		Option postDBOption = Option.builder("postDedupDB").hasArg().required(true).desc("Database post-deduplication").longOpt("postDedupDB").build();

		Options options = new Options();
		CommandLineParser parser = new DefaultParser();

		options.addOption(preDBOption);
		options.addOption(postDBOption);

		String preDedupDB = null;
		String postDedupDB = null;
		try {
			commandLine = parser.parse(options, args);

			System.out.println("----------------------------");
			System.out.println("OPTIONS:");
			
			if(commandLine.hasOption("preDedupDB")) {       	
				preDedupDB =commandLine.getOptionValue("preDedupDB");
	        	System.out.println("\tPre-deduplication DB: " + preDedupDB);
	        } else {
	        	System.out.println("\tPre-deduplication DB name not provided. Use the preDedupDB option.");
	        	System.exit(1);
	        }
			
			if (commandLine.hasOption("postDedupDB")) {
				postDedupDB = commandLine.getOptionValue("postDedupDB");
				System.out.println("\tPost-deduplication DB: " + postDedupDB);
			} else {
				System.out.println("\tPost-deduplication DB name not provided. Use the postDedupDB option.");
				System.exit(1);
			}							

			System.out.println("----------------------------\n");

		} catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		EntityManagerFactory emfactory1 = Persistence.createEntityManagerFactory(preDedupDB);
		EntityManager entitymanager1 = emfactory1.createEntityManager();
		EntityManagerFactory emfactory2 = Persistence.createEntityManagerFactory(postDedupDB);
		EntityManager entitymanager2 = emfactory2.createEntityManager();
		
		Map<String, List<Organization>> sourceOrgsMap = new HashMap<>();
		
		// retrieve organizations from pre-deduplication db and split them by source
		List<Organization> orgs1 = retrieveOrganizations(preDedupDB, entitymanager1);
		Map<String, List<Organization>> sourceOrgsMap1 = groupOrganizationBySource(orgs1);
		// retrieve organizations from post-deduplication db and split them by source
		List<Organization> orgs2 = retrieveOrganizations(postDedupDB, entitymanager2);
		Map<String, List<Organization>> sourceOrgsMap2 = groupOrganizationBySource(orgs2);
		
		// for each source, identify the organizations in the pre db that don't exist in post db 
		for(String sourceName: sourceOrgsMap2.keySet()) {
			
			List<Organization> orgDiff = new ArrayList<>();
			
			// retrieve post-dedup organizations of the current source and index them by id
			List<Organization> sourceOrgs2 = sourceOrgsMap2.get(sourceName);
			Map<Integer, Organization> orgMap2 = new HashMap<>();
			for(Organization org: sourceOrgs2) {
				orgMap2.put(org.getId(), org);
			}
			
			// retrieve pre-dedup organizations of the current source and search them in the previous index
			// if the organization doesn't exist in the index then store the organization 
			List<Organization> sourceOrgs1 = sourceOrgsMap1.get(sourceName);
			for(Organization org: sourceOrgs1) {
				if (!orgMap2.containsKey(org.getId())) {
					orgDiff.add(org);
				}
				
			}
			
			System.out.println(sourceName + ": " + orgDiff.size());
			if (orgDiff.size() < 20) {
				for(Organization org: orgDiff) {
					System.out.println(org.getId() + " " + org.getLabel());
				}
			}
			
			sourceOrgsMap.put(sourceName, orgDiff);
			
		}				
		
		
		
		/*Query query = entitymanager1.createQuery("Select s FROM Source s");
		List<Source> sources = query.getResultList();
		
		entitymanager1.getTransaction().commit();
		
		Map<String, List<Organization>> sourceOrgsMap = new HashMap<>(); 
		for(Source source: sources) {
			List<Organization> orgs1 = retrieveOrganizationsBySource(preDedupDB, entitymanager1, source);
			Map<Integer, Organization> orgMap1 = new HashMap<>();
			for(Organization org: orgs1) {
				orgMap1.put(org.getId(), org);
			}
			
			List<Organization> orgs2 = retrieveOrganizationsBySource(postDedupDB, entitymanager2, source);
			List<Organization> orgDiff = new ArrayList<>();
			for(Organization org: orgs2) {
				if (!orgMap1.containsKey(org.getId())) {
					orgDiff.add(org);
				}
				
			}
			
			System.out.println(source.getLabel() + ": " + orgDiff.size());
			
			sourceOrgsMap.put(source.getLabel(), orgDiff);			
		}
		
		for(String sourceName: sourceOrgsMap.keySet()) {
			System.out.println(sourceName);
			List<Organization> orgs = sourceOrgsMap.get(sourceName);
			for(Organization org: orgs) {
				System.out.println("\t" + org.getLabel());
			}
		}*/
		
		
	}
	
	public static Map<String, List<Organization>> groupOrganizationBySource(List<Organization> organizations) {
		
		Map<String, List<Organization>> orgMap = new HashMap<>();
		for(Organization org: organizations) {
			List<Source> orgSources = org.getSources();
			for(Source source: orgSources ) {
				String sourceName = source.getLabel();
				List<Organization> orgs = new ArrayList<>();
				if (orgMap.containsKey(sourceName)) {
					orgs = orgMap.get(sourceName);					
				}
				orgs.add(org);
				orgMap.put(sourceName, orgs);
			}
		}
		
		return orgMap;
	}
	
	public static List<Organization> retrieveOrganizationsBySource(String dbName, EntityManager entitymanager, Source source) {
		
		entitymanager.getTransaction().begin();
		System.out.println("Starting retrieving organizations of " + source.getLabel() + " source from " + dbName + " database...");
        Query query_org = entitymanager.createQuery("Select o FROM Organization o WHERE :id MEMBER OF o.sources");
        query_org.setParameter("id", source);
        List<Organization> orgs = query_org.getResultList();
        System.out.println("Retrieved " + orgs.size() + " organizations from " + source.getLabel() + " source.");
        entitymanager.getTransaction().commit();
        
        return orgs;
	}
	
	public static List<Organization> retrieveOrganizations(String dbName, EntityManager entitymanager) {
		
		entitymanager.getTransaction().begin();
		System.out.println("Starting retrieving organizations from " + dbName + " database...");
        Query query_org = entitymanager.createQuery("Select o FROM Organization o");
        List<Organization> orgs = query_org.getResultList();
        System.out.println("Retrieved " + orgs.size() + " organizations.");
        entitymanager.getTransaction().commit();
        
        return orgs;
	}

}
