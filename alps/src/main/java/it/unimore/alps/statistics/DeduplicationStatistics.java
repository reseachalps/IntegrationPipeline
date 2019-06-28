package it.unimore.alps.statistics;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Query;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import it.unimore.alps.sql.model.*;

public class DeduplicationStatistics {
	
	private String preDedupDB; 
	private String postDedupDB;
	private String entityTarget;
	private String partitionDimension;
	private String partitionSubDimension;
	private String filterAttribute;
	private String filterSubAttribute;
	private String filterValue;
	private String filterAttributeCount;
	private String countOperator;
	private int filterCountValue;
	private List<String> exportAttributes;
	
	private DBStatistics predbStats;
	private DBStatistics postdbStats;
	private DBStatisticsComparator dbComparatorStats;
	
	public static void main(String[] args) {

		CommandLine commandLine;
		Option preDBOption = Option.builder("preDedupDB").hasArg().required(true).desc("Database pre-deduplication").longOpt("preDedupDB").build();
		Option postDBOption = Option.builder("postDedupDB").hasArg().required(true).desc("Database post-deduplication").longOpt("postDedupDB").build();
		Option entityTargetOption = Option.builder("entityTarget").hasArg().required(true).desc("Entity target for which compute statistics. The possibile choices are 'Organization', 'Project', 'Publication' and 'Person'.").longOpt("entityTarget").build();
		Option partitionDimensionOption = Option.builder("partitionDimension").hasArg().required(true).desc("Partition attribute used to cluster entities.").longOpt("partitionDimension").build();
		Option partitionSubDimensionOption = Option.builder("partitionSubDimension").hasArg().desc("Partition attribute of second level used to cluster entities.").longOpt("partitionSubDimension").build();
		Option filterAttributeOption = Option.builder("filterAttribute").hasArg().desc("Attribute for filtering.").longOpt("filterAttribute").build();
		Option filterSubAttributeOption = Option.builder("filterSubAttribute").hasArg().desc("Sub-attribute for filtering.").longOpt("filterSubAttribute").build();
		Option filterValueOption = Option.builder("filterValue").hasArg().desc("Value for filtering.").longOpt("filterValue").build();
		Option filterAttributeCountOption = Option.builder("filterAttributeCount").hasArg().desc("Attribute for count filtering.").longOpt("filterAttributeCount").build();
		Option countOperatorOption = Option.builder("countOperator").hasArg().desc("Operator for count filtering. The choices available are 'equality', 'greater', 'lower'.").longOpt("countOperator").build();
		Option filterCountValueOption = Option.builder("filterCountValue").hasArg().desc("Value for count filtering.").longOpt("filterCountValue").build();
		Option exportAttributesOption = Option.builder("exportAttributes").hasArg().desc("Comma-separated attributes to be exported in statistics.").longOpt("exportAttributes").build();

		Options options = new Options();
		CommandLineParser parser = new DefaultParser();

		options.addOption(preDBOption);
		options.addOption(postDBOption);
		options.addOption(entityTargetOption);
		options.addOption(partitionDimensionOption);
		options.addOption(partitionSubDimensionOption);
		options.addOption(filterAttributeOption);
		options.addOption(filterSubAttributeOption);
		options.addOption(filterValueOption);
		options.addOption(filterAttributeCountOption);
		options.addOption(countOperatorOption);
		options.addOption(filterCountValueOption);
		options.addOption(exportAttributesOption);

		String preDedupDB = null;
		String postDedupDB = null;
		String entityTarget = null;
		String partitionDimension = null;
		String partitionSubDimension = null;
		String filterAttribute = null;
		String filterSubAttribute = null;
		String filterValue = null;
		String filterAttributeCount = null;
		String countOperator = null;
		int filterCountValue = -1;
		String exportAttributes = null;
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
			
			if (commandLine.hasOption("entityTarget")) {
				entityTarget = commandLine.getOptionValue("entityTarget");
				System.out.println("\tEntity target: " + entityTarget);
			} else {
				System.out.println("\tEntity target not provided. Use the entityTarget option.");
				System.exit(1);
			}
			
			if (commandLine.hasOption("partitionDimension")) {
				partitionDimension = commandLine.getOptionValue("partitionDimension");
				System.out.println("\tPartition dimension: " + partitionDimension);
			} else {
				System.out.println("\tPartition dimension not provided. Use the partitionDimension option.");
				System.exit(1);
			}
			
			if (commandLine.hasOption("partitionSubDimension")) {
				partitionSubDimension = commandLine.getOptionValue("partitionSubDimension");
			}
			System.out.println("\tPartition sub-dimension: " + partitionSubDimension);
			
			if (commandLine.hasOption("filterAttribute")) {
				filterAttribute = commandLine.getOptionValue("filterAttribute");				
			}
			System.out.println("\tFilter attribute: " + filterAttribute);
			
			if (commandLine.hasOption("filterSubAttribute")) {
				filterSubAttribute = commandLine.getOptionValue("filterSubAttribute");				
			}
			System.out.println("\tSub-attribute: " + filterSubAttribute);
			
			if (commandLine.hasOption("filterValue")) {
				filterValue = commandLine.getOptionValue("filterValue");				
			}
			System.out.println("\tFilter value: " + filterValue);
			
			if (commandLine.hasOption("filterAttributeCount")) {
				filterAttributeCount = commandLine.getOptionValue("filterAttributeCount");				
			}
			System.out.println("\tFilter count attribute: " + filterAttributeCount);
			
			if (commandLine.hasOption("countOperator")) {
				countOperator = commandLine.getOptionValue("countOperator");				
			}
			System.out.println("\tFilter count operator: " + countOperator);
			
			if (commandLine.hasOption("filterCountValue")) {
				filterCountValue = Integer.parseInt(commandLine.getOptionValue("filterCountValue"));				
			}
			System.out.println("\tFilter count value: " + filterCountValue);
			
			if (commandLine.hasOption("exportAttributes")) {
				exportAttributes = commandLine.getOptionValue("exportAttributes");				
			}
			System.out.println("\tAttributes to be exported: " + exportAttributes);

			System.out.println("----------------------------\n");

		} catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("Starting collecting deduplication statistics...");
		DeduplicationStatistics dedupStats = new DeduplicationStatistics(preDedupDB, postDedupDB, entityTarget, partitionDimension, partitionSubDimension, filterAttribute, filterSubAttribute, filterValue, filterAttributeCount, countOperator, filterCountValue, exportAttributes);
		dedupStats.computeStatistics();
		dedupStats.displayStatistics();
		dedupStats.exportStatistics();
	}
	
	public DeduplicationStatistics(String preDedupDB, String postDedupDB, String entityTarget, String partitionDimension, String partitionSubDimension, String filterAttribute, String filterSubAttribute, String filterValue, String filterAttributeCount, String countOperator, int filterCountValue, String exportAttributes) {
		
		this.preDedupDB = preDedupDB;
		this.postDedupDB = postDedupDB;
		this.entityTarget = entityTarget;
		this.partitionDimension = partitionDimension;
		this.partitionSubDimension = partitionSubDimension;
		this.filterAttribute = filterAttribute;
		this.filterSubAttribute = filterSubAttribute;
		this.filterValue = filterValue;
		this.filterAttributeCount = filterAttributeCount;
		this.countOperator = countOperator;
		this.filterCountValue = filterCountValue;
		List<String> exportAttributesList = null;
		if (exportAttributes != null) {
			exportAttributesList = new ArrayList<String>(Arrays.asList(exportAttributes.split(",")));
		}
		this.exportAttributes = exportAttributesList;

	}		
	
	private void computeStatistics() {		
		
		this.predbStats = new DBStatistics(preDedupDB, entityTarget, partitionDimension, partitionSubDimension, filterAttribute, filterSubAttribute, filterValue, filterAttributeCount, countOperator, filterCountValue, exportAttributes);
		this.predbStats.computeStatistics();
		
		this.postdbStats = new DBStatistics(postDedupDB, entityTarget, partitionDimension, partitionSubDimension, filterAttribute, filterSubAttribute, filterValue, filterAttributeCount, countOperator, filterCountValue, exportAttributes);
		this.postdbStats.computeStatistics();
		
		this.dbComparatorStats = new DBStatisticsComparator(preDedupDB, predbStats, postDedupDB, postdbStats);
		this.dbComparatorStats.compareStatistics();		
	}				
				
	private void displayStatistics() {
		predbStats.displayDBStatistics();
		postdbStats.displayDBStatistics();
		dbComparatorStats.displayStatistics();			
	}
	
		
	private void exportStatistics() {		
		predbStats.exportDBStatistics(exportAttributes);
		postdbStats.exportDBStatistics(exportAttributes);
		dbComparatorStats.exportStatistics(exportAttributes);
	}

}
