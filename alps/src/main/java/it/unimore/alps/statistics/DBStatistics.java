package it.unimore.alps.statistics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.Person;
import it.unimore.alps.sql.model.Project;
import it.unimore.alps.sql.model.Publication;
import it.unimore.alps.utils.CSVFileWriter;

public class DBStatistics {
	private String dbName;
	private String entityTarget;
	private String partitionDimension;
	private String partitionSubDimension;
	private String filterAttribute;
	private String filterSubAttribute;
	private String filterValue;
	private String filterAttributeCount;
	private String countOperator;
	private int filterCountValue;
	private List<String> statsAttributes;
	
	private EntityDescription<Organization> orgStats;
	private EntityDescription<Project> prjStats;
	private EntityDescription<Person> perStats;
	private EntityDescription<Publication> pubStats;
	
	public DBStatistics(String dbName, String entityTarget, String partitionDimension, String partitionSubDimension, String filterAttribute, String filterSubAttribute, String filterValue, String filterAttributeCount, String countOperator, int filterCountValue, List<String> statsAttributes) {
		this.dbName = dbName;
		this.entityTarget = entityTarget;
		this.partitionDimension = partitionDimension;
		this.partitionSubDimension = partitionSubDimension;
		this.filterAttribute = filterAttribute;
		this.filterSubAttribute = filterSubAttribute;
		this.filterValue = filterValue;
		this.filterAttributeCount = filterAttributeCount;
		this.countOperator = countOperator;
		this.filterCountValue = filterCountValue;
		this.statsAttributes = statsAttributes;
	}
	
	public void computeStatistics() {
		
		System.out.println("Starting computing statistics for " + dbName + " database...");
		if (entityTarget.equals("All")) {

			orgStats = new EntityDescription<>(dbName, "Organization", partitionDimension, partitionSubDimension, filterAttribute, filterSubAttribute, filterValue, filterAttributeCount, countOperator, filterCountValue, statsAttributes);
			orgStats.describeEntity();
			prjStats = new EntityDescription<>(dbName, "Project", partitionDimension, partitionSubDimension, filterAttribute, filterSubAttribute, filterValue, filterAttributeCount, countOperator, filterCountValue, statsAttributes);
			prjStats.describeEntity();
			perStats = new EntityDescription<>(dbName, "Person", partitionDimension, partitionSubDimension, filterAttribute, filterSubAttribute, filterValue, filterAttributeCount, countOperator, filterCountValue, statsAttributes);
			perStats.describeEntity();
			pubStats = new EntityDescription<>(dbName, "Publication", partitionDimension, partitionSubDimension, filterAttribute, filterSubAttribute, filterValue, filterAttributeCount, countOperator, filterCountValue, statsAttributes);
			pubStats.describeEntity();

		} else {			
			
			if (entityTarget.equals("Organization")) {
				orgStats = new EntityDescription<>(dbName, entityTarget, partitionDimension, partitionSubDimension, filterAttribute, filterSubAttribute, filterValue, filterAttributeCount, countOperator, filterCountValue, statsAttributes);
				orgStats.describeEntity();
			}
			
			if (entityTarget.equals("Project")) { 
				prjStats = new EntityDescription<>(dbName, entityTarget, partitionDimension, partitionSubDimension, filterAttribute, filterSubAttribute, filterValue, filterAttributeCount, countOperator, filterCountValue, statsAttributes);
				prjStats.describeEntity();
			}
			
			if (entityTarget.equals("Person")) { 
				perStats = new EntityDescription<>(dbName, entityTarget, partitionDimension, partitionSubDimension, filterAttribute, filterSubAttribute, filterValue, filterAttributeCount, countOperator, filterCountValue, statsAttributes);
				perStats.describeEntity();
			}
			
			if (entityTarget.equals("Publication")) { 
				pubStats = new EntityDescription<>(dbName, entityTarget, partitionDimension, partitionSubDimension, filterAttribute, filterSubAttribute, filterValue, filterAttributeCount, countOperator, filterCountValue, statsAttributes);
				pubStats.describeEntity();
			}
		}
		System.out.println("Statistics computation for " + dbName + " database completed.");
		
	}
	
	public EntityDescription<Organization> getOrganizationStatistics() {
		return orgStats;
	}
	
	public EntityDescription<Project> getProjectStatistics() {
		return prjStats;
	}
	
	public EntityDescription<Person> getPersonStatistics() {
		return perStats;
	}
	
	public EntityDescription<Publication> getPublicationStatistics() {
		return pubStats;
	}	
	
	public void displayDBStatistics() {
		System.out.println(dbName);
		if (entityTarget.equals("Organization")) {
			orgStats.displayStatistics();
		}
		if (entityTarget.equals("Project")) {
			prjStats.displayStatistics();
		}
		if (entityTarget.equals("Person")) {
			perStats.displayStatistics();
		}
		if (entityTarget.equals("Publication")) {
			pubStats.displayStatistics();
		}
	}
	
	public void exportDBStatistics(List<String> exportAttributes) {
		if (entityTarget.equals("Organization")) {
			orgStats.exportStatistics(dbName +"_organizations_" + partitionDimension + "_statistics.csv", exportAttributes);
		}
		if (entityTarget.equals("Project")) {
			prjStats.exportStatistics(dbName +"_projects_" + partitionDimension + "_statistics.csv", exportAttributes);
		}
		if (entityTarget.equals("Person")) {
			perStats.exportStatistics(dbName +"_people_" + partitionDimension + "_statistics.csv", exportAttributes);
		}
		if (entityTarget.equals("Publication")) {
			pubStats.exportStatistics(dbName +"_publications_" + partitionDimension + "_statistics.csv", exportAttributes);
		}
	}
	
	public Map<String, Integer> getCount() {
		
		Map<String, Integer> countStats = null;
		
		if (entityTarget.equals("Organization")) {
			countStats = orgStats.getCount();
		}
		if (entityTarget.equals("Project")) {
			countStats = prjStats.getCount();
		}
		if (entityTarget.equals("Person")) {
			countStats = perStats.getCount();
		}
		if (entityTarget.equals("Publication")) {
			countStats = pubStats.getCount();
		}
		
		return countStats;
	}
	
	public Map<String, Map<String, Integer>> getMissingValueFreq() {
		
		Map<String, Map<String, Integer>> missingStats = null;
		
		if (entityTarget.equals("Organization")) {
			missingStats = orgStats.getMissingValueFreq();
		}
		if (entityTarget.equals("Project")) {
			missingStats = prjStats.getMissingValueFreq();
		}
		if (entityTarget.equals("Person")) {
			missingStats = perStats.getMissingValueFreq();
		}
		if (entityTarget.equals("Publication")) {
			missingStats = pubStats.getMissingValueFreq();
		}
		
		return missingStats;
	}

	public Map<String, Map<String, Float>> getMissingValueFreqPercentage() {
		
		Map<String, Map<String, Float>> missingPercentageStats = null;
		
		if (entityTarget.equals("Organization")) {
			missingPercentageStats = orgStats.getMissingValueFreqPercentage();
		}
		if (entityTarget.equals("Project")) {
			missingPercentageStats = prjStats.getMissingValueFreqPercentage();
		}
		if (entityTarget.equals("Person")) {
			missingPercentageStats = perStats.getMissingValueFreqPercentage();
		}
		if (entityTarget.equals("Publication")) {
			missingPercentageStats = pubStats.getMissingValueFreqPercentage();
		}
		
		return missingPercentageStats;
	}
}
