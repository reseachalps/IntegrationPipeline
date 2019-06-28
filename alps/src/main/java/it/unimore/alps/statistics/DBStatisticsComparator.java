package it.unimore.alps.statistics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationIdentifier;
import it.unimore.alps.utils.CSVFileWriter;

public class DBStatisticsComparator {
	private DBStatistics dbStats1;
	private DBStatistics dbStats2;
	private String dbName1;
	private String dbName2;
	
	private Map<String, Integer> diffCountMap;
	private Map<String, Map<String, Integer>> attrMissingValueDifferenceFreqMap;
	private Map<String, Map<String, Float>> attrMissingValueDifferencePercentageFreqMap;
	
	public DBStatisticsComparator(String dbName1, DBStatistics dbStats1, String dbName2, DBStatistics dbStats2) {
		this.dbStats1 = dbStats1;
		this.dbStats2 = dbStats2;
		this.dbName1 = dbName1;
		this.dbName2 = dbName2;
	}
	
	private Map<String, PartitionStatistics<Organization>> getPartitionsStatistics(DBStatistics dbStats) {
		Map<String, PartitionStatistics<Organization>> partStatisticsMap = new HashMap<>();
		EntityDescription<Organization> orgStats = dbStats.getOrganizationStatistics();
		Map<String, PartitionStatistics<Organization>> orgPartitionStatsMap = orgStats.getEntityPartitionStatistics();
		for (String partitionValue: orgPartitionStatsMap.keySet()) {				
			PartitionStatistics<Organization> orgPartitionStats = orgPartitionStatsMap.get(partitionValue);
			partStatisticsMap.put(partitionValue, orgPartitionStats);
		}
		
		return partStatisticsMap;
	}
	
	public void compareStatistics() {
		
		Map<String, Integer> countStats1 = dbStats1.getCount();
		Map<String, Map<String, Integer>> missingStats1 = dbStats1.getMissingValueFreq();
		Map<String, Map<String, Float>> missingPercentageStats1 = dbStats1.getMissingValueFreqPercentage();
		
		Map<String, Integer> countStats2 = dbStats2.getCount();
		Map<String, Map<String, Integer>> missingStats2 = dbStats2.getMissingValueFreq();
		Map<String, Map<String, Float>> missingPercentageStats2 = dbStats2.getMissingValueFreqPercentage();
		
		Map<String, Integer> diffCountMap = new HashMap<>();
		Map<String, Map<String, Integer>> attrMissingValueDifferenceFreqMap = new HashMap<>();
		Map<String, Map<String, Float>> attrMissingValueDifferencePercentageFreqMap = new HashMap<>();
		
		for(String partitionValue: countStats1.keySet()) {	
			
			if (!countStats2.containsKey(partitionValue)) {
				diffCountMap = countStats1;
				attrMissingValueDifferenceFreqMap = missingStats1;
				attrMissingValueDifferencePercentageFreqMap = missingPercentageStats1;
			} else {
					
					Map<String, Integer> attrMissingValueFreq1 = missingStats1.get(partitionValue);
					Map<String, Float> attrMissingValueFreqPercentage1 = missingPercentageStats1.get(partitionValue);
					Map<String, Integer> attrMissingValueFreq2 = missingStats2.get(partitionValue);
					Map<String, Float> attrMissingValueFreqPercentage2 = missingPercentageStats2.get(partitionValue);
					
					diffCountMap.put(partitionValue, countStats1.get(partitionValue) - countStats2.get(partitionValue));
								
					Map<String, Integer> diffMissingValues = new HashMap<>();
					Map<String, Float> diffMissingValuesPercentage = new HashMap<>();
					for (String attr: attrMissingValueFreq1.keySet()) {
						diffMissingValues.put(attr, attrMissingValueFreq1.get(attr) - attrMissingValueFreq2.get(attr));
						diffMissingValuesPercentage.put(attr, attrMissingValueFreqPercentage1.get(attr) - attrMissingValueFreqPercentage2.get(attr));
					}
					attrMissingValueDifferenceFreqMap.put(partitionValue, diffMissingValues);
					attrMissingValueDifferencePercentageFreqMap.put(partitionValue, diffMissingValuesPercentage);
				
			}
			this.diffCountMap = diffCountMap;
			this.attrMissingValueDifferenceFreqMap = attrMissingValueDifferenceFreqMap;
			this.attrMissingValueDifferencePercentageFreqMap = attrMissingValueDifferencePercentageFreqMap;
		}
		
	}
	
	public void displayStatistics() {
		System.out.println("Comparison statistics");
		for (String partitionValue: diffCountMap.keySet()) {
			System.out.println("\t" + partitionValue);
			System.out.println("\t\tCount = " + diffCountMap.get(partitionValue));
			System.out.println("\t\tMissing values");
			Map<String, Integer> diffMissingFreq = attrMissingValueDifferenceFreqMap.get(partitionValue);
			Map<String, Float> diffMissingFreqPercentage = attrMissingValueDifferencePercentageFreqMap.get(partitionValue);
			for(String attr: diffMissingFreq.keySet()) {
				System.out.println("\t\t\t" + attr + " = " + diffMissingFreq.get(attr) + " (" + diffMissingFreqPercentage.get(attr) +"%)");
			}
		}
	}
	
	public void exportStatistics(List<String> filterAttributes) {
		
		String filename = "comparison_" + dbName1 +"_" + dbName2 + ".csv";
		
		System.out.println("Starting exporting statistics...");
		
		List<List<String>> stats = new ArrayList<List<String>>();
			
		List<String> header = new ArrayList<>();
		header.add("");
		header.add("Count");		
		if (filterAttributes != null) {
			header.addAll(filterAttributes);
		} else {
			List<String> partitionsValues = new ArrayList<>(attrMissingValueDifferenceFreqMap.keySet());
			String firstPartitionValue = partitionsValues.get(0);
			List<String> attributes = new ArrayList<>(attrMissingValueDifferenceFreqMap.get(firstPartitionValue).keySet());
			header.addAll(attributes);
		}				
		
		
		stats.add(header);
		
		for(String partitionValue: diffCountMap.keySet()) {					
		
			List<String> row = new ArrayList<String>();
			
			row.add(partitionValue);
			
			row.add(""+diffCountMap.get(partitionValue));
			
			Map<String, Integer> absoluteMissingValue = attrMissingValueDifferenceFreqMap.get(partitionValue);
			Map<String, Float> percentageMissingValue = attrMissingValueDifferencePercentageFreqMap.get(partitionValue);
			
			List<String> exportAttrs = null;
			if (filterAttributes != null) {
				exportAttrs = filterAttributes;
			} else {
				exportAttrs = new ArrayList<>(absoluteMissingValue.keySet());
			}
			
			for(String attr: exportAttrs) {								
								
				String value = "" + absoluteMissingValue.get(attr) + " (" + String.format("%.02f", percentageMissingValue.get(attr)) + ")"; 
				row.add(value);
			}
			
			stats.add(row);
			
		}
		
		CSVFileWriter writer = new CSVFileWriter(filename);
        writer.write(stats);

        System.out.println("Statistics exporting successfully completed.");
	}
	
}
