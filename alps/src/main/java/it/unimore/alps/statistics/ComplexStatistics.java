package it.unimore.alps.statistics;

import java.lang.reflect.Field;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

import it.unimore.alps.sql.model.GeoCoordinate;
import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationIdentifier;
import it.unimore.alps.sql.model.Project;
import it.unimore.alps.sql.model.Source;
import it.unimore.alps.utils.CSVFileWriter;

public class ComplexStatistics {
	
	private String db;
	private String outputFolder;
	
	private EntityManager entitymanager;
	private EntityManagerFactory emfactory;
	
	private List<Organization> organizations;
	private List<Project> projects;
	
	public static void main(String[] args) {
	
		// read parameter for the db
	
		CommandLine commandLine;
	
		Option DB = Option.builder("DB").hasArg().required(true).desc("DB. ").longOpt("DB").build();
		
		Option outputFolderOption = Option.builder("outputFolder")
        		.hasArg()
        		.required(true)
        		.desc("The output folder for the CSV file.")
        		.longOpt("outputFolder")
        		.build();
		
		Option statisticOption = Option.builder("stats")
        		.hasArg()
        		.required(true)
        		.desc("The statistics to be performed.")
        		.longOpt("statistics")
        		.build();
	
		Options options = new Options();
	
		options.addOption(DB);
		options.addOption(outputFolderOption);
		options.addOption(statisticOption);
        
		CommandLineParser parser = new DefaultParser();
	
		String db = null;
		String outputFolder = null;
		String statistic = null;
		try {
			commandLine = parser.parse(options, args);
			
			System.out.println("----------------------------");
			System.out.println("OPTIONS:");
	
			if (commandLine.hasOption("DB")) {
				db = commandLine.getOptionValue("DB");
				System.out.println("Database: " + db);
			} else {
				System.out.println("Source database not provided. Use the DB option.");
				System.exit(1);
			}
			
			if(commandLine.hasOption("outputFolder")) {	        	
	        	outputFolder =commandLine.getOptionValue("outputFolder");
	        	System.out.println("Output folder: " + outputFolder);
	        } else {
	        	System.out.println("\tOutput Folder is not provided. Use the outputFolder option.");
	        	System.exit(1);
	        }
			
			if(commandLine.hasOption("stats")) {	        	
				statistic =commandLine.getOptionValue("stats");
	        	System.out.println("Statistics: " + statistic);
	        } else {
	        	System.out.println("\tStatistics parameter is not provided. Use the stats option.");
	        	System.exit(1);
	        }
			
			System.out.println("----------------------------\n");			
	
		} catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		ComplexStatistics stats = new ComplexStatistics(db, outputFolder);
		
		switch(statistic) {
		  case "groupOrganizationsBySourceAndCountry":
			  stats.countOrganizationsBySourceAndCountry();
			  break;
		  case "groupProjectsBySourceAndCountry":
			  stats.countProjectsBySourceAndCountry();
			  break;
		  case "statsProjectsBySource":
			  stats.statsProjectsBySource();
			  break;
		  case "countOrganizationsWithoutProjectsBySource":
			  stats.countOrganizationsWithoutProjectsBySource();
			  break;
		  case "countOrganizationsWithProjectsBySource":
			  stats.countOrganizationsWithProjectsBySource();
			  break;
		  case "top10OrganizationWithProjects":
			  stats.top10OrganizationWithProjects();
			  break;
		  case "OrganizationWithoutNutsBySourceAndCountry":
			  stats.OrganizationWithAttributeNullBySourceAndCountry("Nuts");
			  break;
		  case "OrganizationWithoutAddressBySourceAndCountry":
			  stats.OrganizationWithAttributeNullBySourceAndCountry("Address");
			  break;
		  case "OrganizationWithoutCityBySourceAndCountry":
			  stats.OrganizationWithAttributeNullBySourceAndCountry("City");
			  break;
		  case "OrganizationWithoutPostcodeBySourceAndCountry":
			  stats.OrganizationWithAttributeNullBySourceAndCountry("Postcode");
			  break;
		  case "OrganizationWithoutWebsiteBySourceAndCountry":
			  stats.OrganizationWithAttributeNullBySourceAndCountry("Website");
			  break;
		  case "getGlobalInformationCoverage":
			  stats.getGlobalInformationCoverage();
			  break;
		  case "top10ProjectWithOrganizations":
			  stats.top10ProjectWithOrganizations();
			  break;
		  case "top10OrganizationByAttribute":
			  stats.top10OrganizationByAttribute();
			  break;
		  case "All":
			  stats.countOrganizationsBySourceAndCountry();
			  stats.countProjectsBySourceAndCountry();
			  stats.statsProjectsBySource();
			  stats.countOrganizationsWithoutProjectsBySource();
			  stats.countOrganizationsWithProjectsBySource();
			  stats.top10OrganizationWithProjects();
			  stats.OrganizationWithAttributeNullBySourceAndCountry("Nuts");
			  stats.OrganizationWithAttributeNullBySourceAndCountry("Address");
			  stats.OrganizationWithAttributeNullBySourceAndCountry("City");
			  stats.OrganizationWithAttributeNullBySourceAndCountry("Postcode");
			  stats.OrganizationWithAttributeNullBySourceAndCountry("Website");
			  stats.getGlobalInformationCoverage();
			  stats.top10ProjectWithOrganizations();
			  stats.top10OrganizationByAttribute();
			  break;
		  default:
			  // default
			  
		}	
						
	}
	
	public ComplexStatistics(String db, String outputFolder) {
		
		this.db = db;
		this.outputFolder = outputFolder;
		
		this.emfactory = Persistence.createEntityManagerFactory(db);
		this.entitymanager = emfactory.createEntityManager();						
		
	}
	
	private void initializeStatistics() {
		this.entitymanager.getTransaction().begin();		
	}
	
	private void teardownStatistics() {
		this.entitymanager.getTransaction().commit();
		this.entitymanager.close();
		this.emfactory.close();
	}
	
	private void retrieveOrganizations() {
		
		if (this.organizations == null) {

			System.out.println("Starting retrieving organizations...");
			Query queryOrg = entitymanager.createQuery("Select o FROM Organization o");
		    List<Organization> orgs = queryOrg.getResultList();
		    System.out.println("Retrieved " + orgs.size() + " organizations.");
		    
		    this.organizations = orgs;
		    
		}
	}
	
	private void retrieveProjects() {
		
		if (this.projects == null) {

			System.out.println("Starting retrieving projects...");
			Query queryPrj = entitymanager.createQuery("Select p FROM Project p");
		    List<Project> prjs = queryPrj.getResultList();
		    System.out.println("Retrieved " + prjs.size() + " projects.");
		    
		    this.projects = prjs;
		    
		}
	}
	
	public void countOrganizationsBySourceAndCountry() {
		
		// retrieve organizations from the DB
		retrieveOrganizations();
		
		Map<String, Map<String,Integer>> orgSourceCountryCount = new HashMap<>();
		Map<String, Integer> orgPerSourceCount = new HashMap<>();
		Map<String, Integer> orgPerCountryCount = new HashMap<>();
		Set<String> countries = new HashSet<String>(); 
		
		// loop over organizations
		for (Organization org: this.organizations) {
			
			// get organization country code
			String orgCountryCode = org.getCountryCode();
			// save unique countries
			countries.add(orgCountryCode);
			
			// count organizations grouped by country
			if (!orgPerCountryCount.containsKey(orgCountryCode)) {
				orgPerCountryCount.put(orgCountryCode, 1);
			} else {
				int countOrgCountry = orgPerCountryCount.get(orgCountryCode);
				orgPerCountryCount.put(orgCountryCode, countOrgCountry+1);
			}
			
			// loop over organization sources
			for (Source source: org.getSources()) {
				String sourceName = source.getLabel();
				
				// count organizations grouped by source
				if (!orgPerSourceCount.containsKey(sourceName)) {
					orgPerSourceCount.put(sourceName, 1);
				} else {
					int countOrgSource = orgPerSourceCount.get(sourceName);
					orgPerSourceCount.put(sourceName, countOrgSource+1);
				}
				
				// count organizations grouped by country and source
				if (!orgSourceCountryCount.containsKey(sourceName)) {
					Map<String,Integer> orgCountryCount = new HashMap<>();
					orgCountryCount.put(orgCountryCode, 1);
					orgSourceCountryCount.put(sourceName, orgCountryCount);
				} else {
					
					Map<String,Integer> orgCountryCount = orgSourceCountryCount.get(sourceName);
					
					if (!orgCountryCount.containsKey(orgCountryCode)) {
						orgCountryCount.put(orgCountryCode, 1);
					} else {
						int countOrgCountry = orgCountryCount.get(orgCountryCode);
						orgCountryCount.put(orgCountryCode, countOrgCountry+1);
					}
					
					orgSourceCountryCount.put(sourceName, orgCountryCount);
				}
				
			}
			
		}
		
		// export statistic in a CSV file
		List<List<String>> orgsData = new ArrayList<List<String>>();		

		// CSV header
		List<String> header = new ArrayList<>();
		header.add("");
		for(String country: countries) {
			header.add(country);
		}
		header.add("total");		
		orgsData.add(header);

		// CSV content
		for (String sourceName: orgSourceCountryCount.keySet()) {
			
			// CSV content row
			List<String> sourceStat = new ArrayList<>();
			
			// source name
			sourceStat.add(sourceName);
			
			// organization count grouped by country
			Map<String, Integer> sourceCountGroupedByCountry = orgSourceCountryCount.get(sourceName);			
			for (String country: countries) {
				int count = 0;
				if (sourceCountGroupedByCountry.containsKey(country)) {
					count = sourceCountGroupedByCountry.get(country);
				}
				sourceStat.add(Integer.toString(count));
			}
			
			// total
			int sourceOrgs = orgPerSourceCount.get(sourceName);
			sourceStat.add(Integer.toString(sourceOrgs));

			orgsData.add(sourceStat);

		}	
		
		// total grouped by country
		List<String> totStat = new ArrayList<>();
		totStat.add("total");
		
		for (String country: countries) {
			int count = 0;
			if (orgPerCountryCount.containsKey(country)) {
				count = orgPerCountryCount.get(country);
			}
			totStat.add(Integer.toString(count));
		}
		
		int tot = 0;
		for (String source: orgPerSourceCount.keySet()) {
			int sourceCount = orgPerSourceCount.get(source);
			tot += sourceCount;
		}
		totStat.add(Integer.toString(tot));
		orgsData.add(totStat);
		
    	CSVFileWriter writer = new CSVFileWriter(this.outputFolder+"CountOrganizationsBySourceAndCountry.csv");
        writer.write(orgsData);
		
	    System.out.println("Statistics generated successfully.");	    
		
	}
	
	public void countProjectsBySourceAndCountry() {
		
		// retrieve organizations from the DB
		retrieveOrganizations();		
		
		Map<Project, Boolean> uniquePrj = new HashMap<>();
		Map<String, Map<String,Integer>> prjSourceCountryCount = new HashMap<>();
		Map<String, Integer> prjPerSourceCount = new HashMap<>();
		Map<String, Integer> prjPerCountryCount = new HashMap<>();
		Set<String> countries = new HashSet<String>(); 
		
		// loop over organizations
		for (Organization org: this.organizations) {			
			
			// get organization country code
			String orgCountryCode = org.getCountryCode();
			// save unique countries
			countries.add(orgCountryCode);
			
			for (Project prj: org.getProjects()) {
				
				// count projects grouped by country
				if (!prjPerCountryCount.containsKey(orgCountryCode)) {
					prjPerCountryCount.put(orgCountryCode, 1);
				} else {
					int countPrjCountry = prjPerCountryCount.get(orgCountryCode);
					prjPerCountryCount.put(orgCountryCode, countPrjCountry+1);
				}
				
				// loop over project sources
				for (Source source: prj.getSources()) {
					String sourceName = source.getLabel();
					
					// count projects grouped by source
					if (!prjPerSourceCount.containsKey(sourceName)) {
						prjPerSourceCount.put(sourceName, 1);
					} else {
						int countOrgSource = prjPerSourceCount.get(sourceName);
						prjPerSourceCount.put(sourceName, countOrgSource+1);
					}
					
					// count projects grouped by country and source
					if (!prjSourceCountryCount.containsKey(sourceName)) {
						Map<String,Integer> prjCountryCount = new HashMap<>();
						prjCountryCount.put(orgCountryCode, 1);
						prjSourceCountryCount.put(sourceName, prjCountryCount);
					} else {
						
						Map<String,Integer> prjCountryCount = prjSourceCountryCount.get(sourceName);
						
						if (!prjCountryCount.containsKey(orgCountryCode)) {
							prjCountryCount.put(orgCountryCode, 1);
						} else {
							int countPrjCountry = prjCountryCount.get(orgCountryCode);
							prjCountryCount.put(orgCountryCode, countPrjCountry+1);
						}
						
						prjSourceCountryCount.put(sourceName, prjCountryCount);
					}
				}
				
			}
		}
		
		// export statistic in a CSV file
		List<List<String>> prjsData = new ArrayList<List<String>>();		

		// CSV header
		List<String> header = new ArrayList<>();
		header.add("");
		for(String country: countries) {
			header.add(country);
		}
		header.add("total");		
		prjsData.add(header);

		// CSV content
		for (String sourceName: prjSourceCountryCount.keySet()) {
			
			// CSV content row
			List<String> sourceStat = new ArrayList<>();
			
			// source name
			sourceStat.add(sourceName);
			
			// project count grouped by country
			Map<String, Integer> sourceCountGroupedByCountry = prjSourceCountryCount.get(sourceName);			
			for (String country: countries) {
				int count = 0;
				if (sourceCountGroupedByCountry.containsKey(country)) {
					count = sourceCountGroupedByCountry.get(country);
				}
				sourceStat.add(Integer.toString(count));
			}
			
			// total
			int sourceOrgs = prjPerSourceCount.get(sourceName);
			sourceStat.add(Integer.toString(sourceOrgs));

			prjsData.add(sourceStat);

		}	
		
		// total grouped by country
		List<String> totStat = new ArrayList<>();
		totStat.add("total");
		
		for (String country: countries) {
			int count = 0;
			if (prjPerCountryCount.containsKey(country)) {
				count = prjPerCountryCount.get(country);
			}
			totStat.add(Integer.toString(count));
		}
		
		int tot = 0;
		for (String source: prjPerSourceCount.keySet()) {
			int sourceCount = prjPerSourceCount.get(source);
			tot += sourceCount;
		}
		totStat.add(Integer.toString(tot));
		prjsData.add(totStat);
		
    	CSVFileWriter writer = new CSVFileWriter(this.outputFolder+"CountProjectsBySourceAndCountry.csv");
        writer.write(prjsData);
		
	    System.out.println("Statistics generated successfully.");	    
		
	}
	
	public void statsProjectsBySource() {
		
		// retrieve projects and organizations from the DB
		retrieveProjects();	
		retrieveOrganizations();
		
		Map<String, Map<Project, Integer>> uniquePrjPerSource = new HashMap<>();
		Map<String, Integer> prjPerSourceCount = new HashMap<>();
		Map<String, Integer> connectedPrjPerSourceCount = new HashMap<>();
		
		// loop over organizations
		for (Project prj: this.projects) {			
					
			// loop over project sources
			for (Source source: prj.getSources()) {
				String sourceName = source.getLabel();											
				
				// count projects grouped by source
				if (!prjPerSourceCount.containsKey(sourceName)) {
					prjPerSourceCount.put(sourceName, 1);
				} else {
					int countPrjSource = prjPerSourceCount.get(sourceName);
					prjPerSourceCount.put(sourceName, countPrjSource+1);
				}	
			}
		}
			
		
		// loop over organizations
		for (Organization org: this.organizations) {			
					
			// loop over organization projects
			for (Project prj: org.getProjects()) {

				for(Source source: prj.getSources()) {
					String sourceName = source.getLabel();
					
					if (!uniquePrjPerSource.containsKey(sourceName) || !uniquePrjPerSource.get(sourceName).containsKey(prj)) {
					
						// count distinct projects grouped by source
						if (!connectedPrjPerSourceCount.containsKey(sourceName)) {
							connectedPrjPerSourceCount.put(sourceName, 1);
						} else {
							int countPrjSource = connectedPrjPerSourceCount.get(sourceName);
							connectedPrjPerSourceCount.put(sourceName, countPrjSource+1);
						}																	
					}				
					
					Map<Project, Integer> prjMap = uniquePrjPerSource.get(sourceName);
					if (prjMap == null) {
						prjMap = new HashMap<>();
						prjMap.put(prj, 1);
					} else {
						if (prjMap.containsKey(prj)) {
							int prjPerSource = prjMap.get(prj);
							prjMap.put(prj, prjPerSource+1);
						} else {
							prjMap.put(prj, 1);
						}
					}
					uniquePrjPerSource.put(sourceName, prjMap);
					
				}
					
			}
																
		}					
		
		// export statistic in a CSV file
		List<List<String>> prjsData = new ArrayList<List<String>>();		

		// CSV header
		List<String> header = new ArrayList<>();
		header.add("");
		List<String> statMeasures = new ArrayList<>(Arrays.asList("Tot. Projects", "Tot. Projects Connected", "Projects Connected (percentage)", "Avg. Participants in Project", "Min. Participants in Project", "Max. Participants in Project")); 
		for(String statMeasure: statMeasures) {
			header.add(statMeasure);
		}		
		prjsData.add(header);

		// CSV content
		int totalProjects = 0;
		int totalConnectedProjects = 0;
		float totalAvgParticipants = 0;
		float totalMinParticipants = Float.MAX_VALUE;
		float totalMaxParticipants = 0;
		
		for (String sourceName: prjPerSourceCount.keySet()) {
			
			// CSV content row
			List<String> sourceStat = new ArrayList<>();
			
			// source name
			sourceStat.add(sourceName);
			
			// total projects
			int totPrjs = 0;
			if (prjPerSourceCount.get(sourceName) != null) {
				totPrjs = prjPerSourceCount.get(sourceName);
			}
			sourceStat.add(Integer.toString(totPrjs));
			totalProjects += totPrjs;
									
			// total projects connected
			int connectedPrjs = 0;
			if (connectedPrjPerSourceCount.get(sourceName) != null) {
				connectedPrjs = connectedPrjPerSourceCount.get(sourceName);
			}
			sourceStat.add(Integer.toString(connectedPrjs));
			totalConnectedProjects += connectedPrjs;
			
			// percentage project
			float percentageConnectedPrjs = 0;
			if (totPrjs != 0) {
				percentageConnectedPrjs = (((float)connectedPrjs)/totPrjs)*100;
			}
			sourceStat.add(Float.toString(percentageConnectedPrjs));
			
			// participants statistics
			float avgParticipants = 0;
			float minParticipants = Float.MAX_VALUE;
			float maxParticipants = 0;
			Map<Project, Integer> participantStats = uniquePrjPerSource.get(sourceName);
			if (participantStats != null) {
				for (Project prj: participantStats.keySet()) {
					int count = participantStats.get(prj);
					avgParticipants += count;
					if (count < minParticipants) {
						minParticipants = count;
					}
					if (count > maxParticipants) {
						maxParticipants = count;
					}
				}
				totalAvgParticipants += avgParticipants;
				avgParticipants = avgParticipants / connectedPrjs;				
				if (minParticipants < totalMinParticipants) {
					totalMinParticipants = minParticipants;
				}
				if (maxParticipants > totalMaxParticipants) {
					totalMaxParticipants = maxParticipants;
				}
			}
			sourceStat.add(Float.toString(avgParticipants));
			sourceStat.add(Float.toString(minParticipants));
			sourceStat.add(Float.toString(maxParticipants));			
			prjsData.add(sourceStat);

		}	
		
		// total stats
		List<String> totStat = new ArrayList<>();
		totStat.add("total");
		totStat.add(Integer.toString(totalProjects));
		totStat.add(Integer.toString(totalConnectedProjects));
		float totalPercentageConnectedPrjs = 0;
		if (totalProjects != 0) {
			totalPercentageConnectedPrjs = (((float)totalConnectedProjects)/totalProjects)*100;
		}
		totStat.add(Float.toString(totalPercentageConnectedPrjs));
		totStat.add(Float.toString(((float)totalAvgParticipants) / totalConnectedProjects));
		totStat.add(Float.toString(totalMinParticipants));
		totStat.add(Float.toString(totalMaxParticipants));
		

		prjsData.add(totStat);
		
    	CSVFileWriter writer = new CSVFileWriter(this.outputFolder+"statsProjectBySource.csv");
        writer.write(prjsData);
		
	    System.out.println("Statistics generated successfully.");	    
		
	}
	
	
	public void countOrganizationsWithoutProjectsBySource() {
		
		// retrieve organizations from the DB
		retrieveOrganizations();
		
		Map<String, Integer> orgPerSourceCount = new HashMap<>();
		Map<String, Integer> orgWithoutPrjsPerSourceCount = new HashMap<>();
		Set<String> sources = new HashSet<String>(); 
		
		// loop over organizations
		for (Organization org: this.organizations) {						
			
			// loop over organization sources
			for (Source source: org.getSources()) {
				String sourceName = source.getLabel();
				sources.add(sourceName);
					
				// count organizations grouped by source
				if (!orgPerSourceCount.containsKey(sourceName)) {
					orgPerSourceCount.put(sourceName, 1);
				} else {
					int countOrgSource = orgPerSourceCount.get(sourceName);
					orgPerSourceCount.put(sourceName, countOrgSource+1);
				}							
				
				if (org.getProjects() == null || org.getProjects().size() == 0) {
					if (!orgWithoutPrjsPerSourceCount.containsKey(sourceName)) {
						orgWithoutPrjsPerSourceCount.put(sourceName, 1);
					} else {
						int countOrgSource = orgWithoutPrjsPerSourceCount.get(sourceName);
						orgWithoutPrjsPerSourceCount.put(sourceName, countOrgSource+1);
					}
				}				
			}
			
		}
		
		// export statistic in a CSV file
		List<List<String>> orgsData = new ArrayList<List<String>>();		

		// CSV header
		List<String> header = new ArrayList<>();
		header.add("");
		header.add("Num. Org without projects");
		header.add("Total Num. Org");
		header.add("Num. Org without projects (percentage)");		
		orgsData.add(header);

		// CSV content		
		int totalOrgCount = 0;
		int totalOrgNoPrjCount = 0;
		for (String sourceName: sources) {
			
			// CSV content row
			List<String> sourceStat = new ArrayList<>();
			
			// source name
			sourceStat.add(sourceName);
			
			// organization without projects
			int countNoPrj = 0;
			if (orgWithoutPrjsPerSourceCount.get(sourceName) != null) {
				countNoPrj = orgWithoutPrjsPerSourceCount.get(sourceName);
			}
			sourceStat.add(Integer.toString(countNoPrj));
			totalOrgNoPrjCount += countNoPrj;
			
			// organization count
			int count = 0;
			if (orgPerSourceCount.get(sourceName) != null) {
				count = orgPerSourceCount.get(sourceName);
			}
			sourceStat.add(Integer.toString(count));
			totalOrgCount += count;
			
			// organization without projects (percentage)
			float percentageOrgWithoutPrjs = 0;
			if (count != 0) {
				percentageOrgWithoutPrjs = (((float)countNoPrj)/count)*100;
			}
			sourceStat.add(Float.toString(percentageOrgWithoutPrjs));

			orgsData.add(sourceStat);

		}	
		
		// total summary
		List<String> totStat = new ArrayList<>();
		totStat.add("total");
		totStat.add(Integer.toString(totalOrgNoPrjCount));
		totStat.add(Integer.toString(totalOrgCount));
		float percentageOrgWithoutPrjs = 0;
		if (totalOrgCount != 0) {
			percentageOrgWithoutPrjs = (((float)totalOrgNoPrjCount)/totalOrgCount)*100;
		}
		totStat.add(Float.toString(percentageOrgWithoutPrjs));		
		orgsData.add(totStat);
		
    	CSVFileWriter writer = new CSVFileWriter(this.outputFolder+"countOrganizationsWithoutProjectsBySource.csv");
        writer.write(orgsData);
		
	    System.out.println("Statistics generated successfully.");	    
		
	}
	
	
	public void countOrganizationsWithProjectsBySource() {
		
		// retrieve organizations from the DB
		retrieveOrganizations();
		
		Map<String, Integer> orgPerSourceCount = new HashMap<>();
		Map<String, Map<Organization, Integer>> orgWithPrjPerSourceCount = new HashMap<>();
		//Map<String, Integer> orgWithoutPrjsPerSourceCount = new HashMap<>();
		Set<String> sources = new HashSet<String>(); 
		
		// loop over organizations
		for (Organization org: this.organizations) {						
			
			// loop over organization sources
			for (Source source: org.getSources()) {
				String sourceName = source.getLabel();
				sources.add(sourceName);
					
				// count organizations grouped by source
				if (!orgPerSourceCount.containsKey(sourceName)) {
					orgPerSourceCount.put(sourceName, 1);
				} else {
					int countOrgSource = orgPerSourceCount.get(sourceName);
					orgPerSourceCount.put(sourceName, countOrgSource+1);
				}							
				
				// count projects connected to organization
				if (org.getProjects() != null) {
					Map<Organization, Integer> orgPrjsMap = null;
					if (!orgWithPrjPerSourceCount.containsKey(sourceName)) {
						orgPrjsMap = new HashMap<>();
					} else {
						orgPrjsMap = orgWithPrjPerSourceCount.get(sourceName);						
					}
					orgPrjsMap.put(org, org.getProjects().size());
					orgWithPrjPerSourceCount.put(sourceName, orgPrjsMap);
				}				
			}
			
		}
		
		// export statistic in a CSV file
		List<List<String>> orgsData = new ArrayList<List<String>>();		

		// CSV header
		List<String> header = new ArrayList<>();
		header.add("Source");
		header.add("Avg. Projects");
		header.add("Min. Projects");
		header.add("Max. Projects");		
		orgsData.add(header);

		// CSV content		
		int totalOrgCount = 0;
		int totalOrgPrjCount = 0;
		int totalMinNumPrj = Integer.MAX_VALUE;
		int totalMaxNumPrj = 0;
		for (String sourceName: sources) {
			
			int orgPerSource = orgPerSourceCount.get(sourceName);
			totalOrgCount += orgPerSource;
			
			// CSV content row
			List<String> sourceStat = new ArrayList<>();
			
			// source name
			sourceStat.add(sourceName);
			
			// avg/min/max projects per organization						
			float countNumPrj = 0;
			int minNumPrj = Integer.MAX_VALUE;
			int maxNumPrj = 0;
			Map<Organization, Integer> orgPrjsMap = orgWithPrjPerSourceCount.get(sourceName);
			for (Organization org: orgPrjsMap.keySet()) {
				int prjs = orgPrjsMap.get(org);
				countNumPrj += prjs;
				if (prjs < minNumPrj) {
					minNumPrj = prjs;
				}
				if (prjs > maxNumPrj) {
					maxNumPrj = prjs;
				}
			}
			
			totalOrgPrjCount += countNumPrj;
			float avgPrjs = ((float)countNumPrj)/orgPerSource; 
			sourceStat.add(Float.toString(avgPrjs));
			sourceStat.add(Integer.toString(minNumPrj));
			sourceStat.add(Integer.toString(maxNumPrj));
			
			if (minNumPrj < totalMinNumPrj) {
				totalMinNumPrj = minNumPrj;
			}
			
			if (maxNumPrj > totalMaxNumPrj) {
				totalMaxNumPrj = maxNumPrj;
			}

			orgsData.add(sourceStat);

		}	
		
		// total summary
		List<String> totStat = new ArrayList<>();
		totStat.add("total");
		float avgPrjs = ((float)totalOrgPrjCount)/totalOrgCount; 
		totStat.add(Float.toString(avgPrjs));		
		totStat.add(Integer.toString(totalMinNumPrj));
		totStat.add(Integer.toString(totalMaxNumPrj));		
		orgsData.add(totStat);
		
    	CSVFileWriter writer = new CSVFileWriter(this.outputFolder+"countOrganizationsWithProjectsBySource.csv");
        writer.write(orgsData);
		
	    System.out.println("Statistics generated successfully.");	    
		
	}
	
	
	public void top10OrganizationWithProjects() {
		// retrieve organizations from the DB
		retrieveOrganizations();
		
		List<Map.Entry<Organization, Integer>> list =
                new LinkedList<Map.Entry<Organization, Integer>>();
		
		// loop over organizations
		for (Organization org: this.organizations) {												
				
			// count projects connected to organization
			if (org.getProjects() != null) {
				list.add(new AbstractMap.SimpleEntry<Organization, Integer>(org, org.getProjects().size()));
			}				
			
		}
		
		// sort organizations by num projects
		Collections.sort(list, Collections.reverseOrder(new Comparator<Map.Entry<Organization, Integer>>() {
            	
				public int compare(Map.Entry<Organization, Integer> o1, Map.Entry<Organization, Integer> o2) {
            		return (o1.getValue()).compareTo(o2.getValue());
            	}

        }));						
		
		// export statistic in a CSV file
		List<List<String>> orgsData = new ArrayList<List<String>>();		

		// CSV header
		List<String> header = new ArrayList<>();
		header.add("Source");
		header.add("Organization");
		header.add("Country");
		header.add("Num. Projects");		
		orgsData.add(header);

		// CSV content
		int topK = 10;
		int num = 0;
		for (Map.Entry<Organization, Integer> entry : list) {
			
			num += 1;
			
			Organization org = entry.getKey();
			int prjCount = entry.getValue();
			
			// CSV content row
			List<String> orgStat = new ArrayList<>();
			
			// organization sources
			List<String> sourceList = new ArrayList<String>();
			for (Source source: org.getSources()) {
				sourceList.add(source.getLabel());						
			}			
			orgStat.add(String.join(",", sourceList));
			
			// org label
			orgStat.add(org.getLabel());
			
			// org country code
			orgStat.add(org.getCountryCode());
			
			// num projects connected to the organization
			orgStat.add(Integer.toString(prjCount));
			
			orgsData.add(orgStat);
			
			if (num == topK) {
				break;
			}
        }				
		
    	CSVFileWriter writer = new CSVFileWriter(this.outputFolder+"top10OrganizationWithProjects.csv");
        writer.write(orgsData);
		
	    System.out.println("Statistics generated successfully.");
	}
	
	
	public void top10ProjectWithOrganizations() {
		// retrieve organizations from the DB
		retrieveOrganizations();
				
		Map<Project, Integer> projectParticipants = new HashMap<>();
		
		// loop over organizations
		for (Organization org: this.organizations) {
			
			if (org.getProjects() != null) {
				
				// count organizations participating to the project
				for (Project prj : org.getProjects()) {
					if (!projectParticipants.containsKey(prj)) {
						projectParticipants.put(prj, 1);
					} else {
						int numParticipants = projectParticipants.get(prj);
						projectParticipants.put(prj, numParticipants + 1);
					}
				}
			}													
		}
		
		
		// sort projects by num participants
		List<Map.Entry<Project, Integer>> listParticipants = new LinkedList<Map.Entry<Project, Integer>>(projectParticipants.entrySet());			
		Collections.sort(listParticipants, Collections.reverseOrder(new Comparator<Map.Entry<Project, Integer>>() {
            	
				public int compare(Map.Entry<Project, Integer> o1, Map.Entry<Project, Integer> o2) {
            		return (o1.getValue()).compareTo(o2.getValue());
            	}

        }));						
		
		// export statistic in a CSV file
		List<List<String>> prjsData = new ArrayList<List<String>>();		

		// CSV header
		List<String> header = new ArrayList<>();
		header.add("Source");
		header.add("Project");
		header.add("Num. Organizations");		
		prjsData.add(header);

		// CSV content
		int topK = 10;
		int num = 0;
		for (Map.Entry<Project, Integer> entry : listParticipants) {
			
			num += 1;
			
			Project prj = entry.getKey();
			int orgCount = entry.getValue();
			
			// CSV content row
			List<String> prjStat = new ArrayList<>();
			
			// project sources
			List<String> sourceList = new ArrayList<String>();
			for (Source source: prj.getSources()) {
				sourceList.add(source.getLabel());						
			}			
			prjStat.add(String.join(",", sourceList));
			
			// project label
			prjStat.add(prj.getLabel());		
			
			// num organizations connected to the project
			prjStat.add(Integer.toString(orgCount));
			
			prjsData.add(prjStat);
			
			if (num == topK) {
				break;
			}
        }				
		
    	CSVFileWriter writer = new CSVFileWriter(this.outputFolder+"top10ProjectWithOrganizations.csv");
        writer.write(prjsData);
		
	    System.out.println("Statistics generated successfully.");
	}
	
	public void top10OrganizationByAttribute() {
		// retrieve organizations from the DB
		retrieveOrganizations();
			
		//Map<String, List<Map.Entry<String, Integer>>> topOrgPerAttribute = new HashMap<>();
		Map<String, Map<String, Integer>> countOrgPerAttribute = new HashMap<>();
		
		// check the existence of the attributes		
		Organization firstOrg = this.organizations.get(0);
		List<String> attributes = new ArrayList<>(Arrays.asList("city", "postcode", "nutsLevel1", "nutsLevel2", "nutsLevel3"));
		for (String attribute: attributes) {
			if (!checkIfAttributeBelongToEntity(firstOrg, attribute)) {
				System.out.println("Attribute " + attribute + " doesn't appartain to " + firstOrg.getClass().getSimpleName() + " entity type.");
				System.exit(1);
			}
			//topOrgPerAttribute.put(attribute, new LinkedList<Map.Entry<String, Integer>>());
			countOrgPerAttribute.put(attribute, new HashMap<>());
		}
		
		// loop over organizations
		for (Organization org: this.organizations) {	
						
			for (String attribute: attributes) {
				
				try {
					Object attributeValObject = (Object) getEntityAttributeValue(org, attribute);

					if (attributeValObject != null) {						
						
						String attributeVal = (String) attributeValObject;
						Map<String, Integer> topOrgs = countOrgPerAttribute.get(attribute);
						if (!topOrgs.containsKey(attributeVal)) {
							topOrgs.put(attributeVal, 1);
						} else {
							int count = topOrgs.get(attributeVal);
							topOrgs.put(attributeVal, count + 1);
						}
						countOrgPerAttribute.put(attribute, topOrgs);

					}

				} catch (NoSuchFieldException | SecurityException e) {
					e.printStackTrace();
					System.exit(1);
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
					System.exit(1);
				} catch (IllegalAccessException e) {
					e.printStackTrace();
					System.exit(1);
				}
				
			}											
			
		}
		
		// sort organizations by attribute
		Map<String, List<Map.Entry<String, Integer>>> topOrgPerAttribute = new HashMap<>();
		int topK = 10;
		for(String attribute: countOrgPerAttribute.keySet()) {
			
			List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(countOrgPerAttribute.get(attribute).entrySet());
			Collections.sort(list, Collections.reverseOrder(new Comparator<Map.Entry<String, Integer>>() {
	            	
					public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
	            		return (o1.getValue()).compareTo(o2.getValue());
	            	}
	
	        }));
			
			topOrgPerAttribute.put(attribute, list.subList(0, topK));
						
		}
		
		// export statistic in a CSV file
		
		for (String attribute: attributes) {
			List<List<String>> orgsData = new ArrayList<List<String>>();		
	
			// CSV header
			List<String> header = new ArrayList<>();
			header.add("Value");
			header.add("Count");		
			orgsData.add(header);
						
			// CSV content
			List<Map.Entry<String, Integer>> list = topOrgPerAttribute.get(attribute);
			for (Map.Entry<String, Integer> entry : list) {
				
				String value = entry.getKey();
				int count = entry.getValue();
				
				// CSV content row
				List<String> orgStat = new ArrayList<>();
				orgStat.add(value);				
				orgStat.add(Integer.toString(count));
				orgsData.add(orgStat);
			}
			
			CSVFileWriter writer = new CSVFileWriter(this.outputFolder+"top10Organization" + attribute + ".csv");
	        writer.write(orgsData);
		}			
		
    	
		
	    System.out.println("Statistics generated successfully.");
	}
	
	public void OrganizationWithAttributeNullBySourceAndCountry(String targetAttribute) {
		// retrieve organizations from the DB
		retrieveOrganizations();
		
		Map<String, Map<String,Integer>> orgWithAttributeNullSourceAndCountryCount = new HashMap<>();
		Map<String,Integer> orgWithAttributeNullSourceCount = new HashMap<>();
		Map<String,Integer> orgWithAttributeNullCountryCount = new HashMap<>();
		Map<String,Integer> allOrgCountryCount = new HashMap<>();
		Map<String,Integer> allOrgSourceCount = new HashMap<>();
		Set<String> countries = new HashSet<String>();
		
		// loop over organizations
		for (Organization org: this.organizations) {	
			
			String country = org.getCountryCode();
			countries.add(country);
			
			boolean validCondition = false;	
			switch(targetAttribute) {
			  case "Nuts":
				  if (org.getNutsLevel1() == null && org.getNutsLevel2() == null && org.getNutsLevel3() == null) {
					  validCondition = true;
				  }
				  break;
			  case "Address":
				  if (org.getAddress() == null) {
					  validCondition = true;
				  }
				  break;
			  case "City":
				  if (org.getCity() == null) {
					  validCondition = true;
				  }
				  break;
			  case "Postcode":
				  if (org.getPostcode() == null) {
					  validCondition = true;
				  }
				  break;
			  case "Website":
				  if (org.getLinks() == null || org.getLinks().size() == 0) {
					  validCondition = true;
				  }
				  break;				  
			}
			
			// count all organizations grouped by country
			if (!allOrgCountryCount.containsKey(country)) {
				allOrgCountryCount.put(country, 1);
			} else {
				int countOrgCountry = allOrgCountryCount.get(country);
				allOrgCountryCount.put(country, countOrgCountry+1);
			}
			
			// loop over organization sources
			for (Source source: org.getSources()) {
				String sourceName = source.getLabel();
					
				// count organizations grouped by source
				if (!allOrgSourceCount.containsKey(sourceName)) {
					allOrgSourceCount.put(sourceName, 1);
				} else {
					int countOrgSource = allOrgSourceCount.get(sourceName);
					allOrgSourceCount.put(sourceName, countOrgSource+1);
				}
			}			
				
			if (validCondition) {
				
				// count organizations grouped by country
				if (!orgWithAttributeNullCountryCount.containsKey(country)) {
					orgWithAttributeNullCountryCount.put(country, 1);
				} else {
					int countOrgCountry = orgWithAttributeNullCountryCount.get(country);
					orgWithAttributeNullCountryCount.put(country, countOrgCountry+1);
				}
				
			
				// loop over organization sources
				for (Source source: org.getSources()) {
					String sourceName = source.getLabel();
						
					// count organizations grouped by source
					if (!orgWithAttributeNullSourceCount.containsKey(sourceName)) {
						orgWithAttributeNullSourceCount.put(sourceName, 1);
					} else {
						int countOrgSource = orgWithAttributeNullSourceCount.get(sourceName);
						orgWithAttributeNullSourceCount.put(sourceName, countOrgSource+1);
					}							
					
					// count organizations grouped by country and source
					if (!orgWithAttributeNullSourceAndCountryCount.containsKey(sourceName)) {
						Map<String,Integer> orgCountryCount = new HashMap<>();
						orgCountryCount.put(country, 1);
						orgWithAttributeNullSourceAndCountryCount.put(sourceName, orgCountryCount);
					} else {
						
						Map<String,Integer> orgCountryCount = orgWithAttributeNullSourceAndCountryCount.get(sourceName);
						
						if (!orgCountryCount.containsKey(country)) {
							orgCountryCount.put(country, 1);
						} else {
							int countOrgCountry = orgCountryCount.get(country);
							orgCountryCount.put(country, countOrgCountry+1);
						}
						
						orgWithAttributeNullSourceAndCountryCount.put(sourceName, orgCountryCount);
					}			
				}
				
			}
						
		}
		
		// export statistic in a CSV file
		List<List<String>> orgsData = new ArrayList<List<String>>();		

		// CSV header
		List<String> header = new ArrayList<>();
		header.add("");
		for(String country: countries) {
			header.add(country);
		}
		header.add("Sum");
		header.add("Total");
		orgsData.add(header);

		// CSV content
		for (String sourceName: orgWithAttributeNullSourceAndCountryCount.keySet()) {
			
			// CSV content row
			List<String> sourceStat = new ArrayList<>();
			
			// source name
			sourceStat.add(sourceName);
			
			// organization count grouped by country
			Map<String, Integer> sourceCountGroupedByCountry = orgWithAttributeNullSourceAndCountryCount.get(sourceName);			
			for (String country: countries) {
				int count = 0;
				if (sourceCountGroupedByCountry.containsKey(country)) {
					count = sourceCountGroupedByCountry.get(country);
				}
				sourceStat.add(Integer.toString(count));
			}
			
			// sum
			int sourceOrgs = orgWithAttributeNullSourceCount.get(sourceName);
			sourceStat.add(Integer.toString(sourceOrgs));
			
			// total
			int sourceAllOrgs = allOrgSourceCount.get(sourceName);
			sourceStat.add(Integer.toString(sourceAllOrgs));

			orgsData.add(sourceStat);

		}	
		
		// sum grouped by country
		List<String> sumStat = new ArrayList<>();
		sumStat.add("Sum");
		
		for (String country: countries) {
			int count = 0;
			if (orgWithAttributeNullCountryCount.containsKey(country)) {
				count = orgWithAttributeNullCountryCount.get(country);
			}
			sumStat.add(Integer.toString(count));
		}
		
		int sum = 0;
		for (String source: orgWithAttributeNullSourceCount.keySet()) {
			int sourceCount = orgWithAttributeNullSourceCount.get(source);
			sum += sourceCount;
		}
		sumStat.add(Integer.toString(sum));
		orgsData.add(sumStat);
		
		// total grouped by country
		List<String> totStat = new ArrayList<>();
		totStat.add("Total");
		
		for (String country: countries) {
			int count = 0;
			if (allOrgCountryCount.containsKey(country)) {
				count = allOrgCountryCount.get(country);
			}
			totStat.add(Integer.toString(count));
		}
		totStat.add("");
		
		int tot = 0;
		for (String source: allOrgCountryCount.keySet()) {
			int sourceCount = allOrgCountryCount.get(source);
			tot += sourceCount;
		}
		totStat.add(Integer.toString(tot));
		orgsData.add(totStat);
		
		
    	CSVFileWriter writer = new CSVFileWriter(this.outputFolder+"OrganizationWithout" + targetAttribute + "BySourceAndCountry.csv");
        writer.write(orgsData);
		
	    System.out.println("Statistics generated successfully.");
	}
	
	
	private boolean checkIfAttributeBelongToEntity(Object entity, String attr) {
		
		boolean exist = true;
		try {
			Object value = getEntityAttributeValue(entity, attr);
		} catch (NoSuchFieldException | SecurityException e) {
			exist = false;
		} catch (IllegalArgumentException e) {
			exist = false;
		} catch (IllegalAccessException e) {
			exist = false;
		}
		
		return exist;

	}
	
	private Object getEntityAttributeValue(Object entity, String attr) throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		
		Class<?> entityClass = entity.getClass();
		Field f = entityClass.getDeclaredField(attr);
		f.setAccessible(true);
		Object value = (Object) f.get(entity);								
		
		return value;
	}
	
	
	public void getGlobalInformationCoverage() {
		
		// retrieve organizations from the DB
		retrieveOrganizations();
		
		int numOrgs = this.organizations.size();
		Map<String, Integer> attributeCoverage = new HashMap<>();
		
		// check the existence of the attributes		
		Organization firstOrg = this.organizations.get(0);
		List<String> attributes = new ArrayList<>(Arrays.asList("address", "city", "postcode", "nutsLevel1", "nutsLevel2", "nutsLevel3", "links"));
		for (String attribute: attributes) {
			if (!checkIfAttributeBelongToEntity(firstOrg, attribute)) {
				System.out.println("Attribute " + attribute + " doesn't appartain to " + firstOrg.getClass().getSimpleName() + " entity type.");
				System.exit(1);
			}
		}
				
		// loop over organizations
		for (Organization org: this.organizations) {	
				
			// loop over attributes
			for (String attribute: attributes) {
				
				// check if the organization has a non-null value for the current attribute
				try {
					Object attributeValObject = (Object) getEntityAttributeValue(org, attribute);

					if (attributeValObject != null) {
						if (attributeValObject instanceof Collection<?>){
							if (((List<?>) attributeValObject).size() == 0) {
								continue;
							}
						}
						
						if (!attributeCoverage.containsKey(attribute)) {
							attributeCoverage.put(attribute, 1);
						} else {
							int coverage = attributeCoverage.get(attribute);
							attributeCoverage.put(attribute, coverage + 1);
						}
					}

				} catch (NoSuchFieldException | SecurityException e) {
					e.printStackTrace();
					System.exit(1);
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
					System.exit(1);
				} catch (IllegalAccessException e) {
					e.printStackTrace();
					System.exit(1);
				}

			}
						
		}
		
		// export statistic in a CSV file
		List<List<String>> orgsData = new ArrayList<List<String>>();		

		// CSV header
		List<String> header = new ArrayList<>();
		header.add("");
		header.add("coverage");
		orgsData.add(header);

		// CSV content
		for (String attribute: attributes) {
			
			// CSV content row
			List<String> attrStat = new ArrayList<>();
			
			// attribute name
			attrStat.add(attribute);
			
			// attribute information coverage
			int absoluteCoverage = 0;
			if (attributeCoverage.get(attribute) != null) {
				absoluteCoverage = attributeCoverage.get(attribute);
			}
			float percentageCoverage = (((float)absoluteCoverage) / numOrgs)*100;
			String finalCoverage = absoluteCoverage + " (" + percentageCoverage + ")";
			attrStat.add(finalCoverage);

			orgsData.add(attrStat);

		}					
		
    	CSVFileWriter writer = new CSVFileWriter(this.outputFolder+"GlobalInformationCoverage.csv");
        writer.write(orgsData);
		
	    System.out.println("Statistics generated successfully.");
	}

}
