package it.unimore.alps.statistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.utils.CSVFileWriter;

public class EntityDescription<T> {
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
	
	private EntityManager entitymanager;
	private EntityManagerFactory emfactory;
	
	private List<T> entities;
	private List<T> filteredEntities;
	
	private Map<String, PartitionStatistics<T>> entityPartitionStats;
	
	public EntityDescription(String dbName, String entityTarget, String partitionDimension, String partitionSubDimension, String filterAttribute, String filterSubAttribute, String filterValue, String filterAttributeCount, String countOperator, int filterCountValue, List<String> statsAttributes) {
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
		this.entityPartitionStats = new HashMap<>();
		
		this.emfactory = Persistence.createEntityManagerFactory(dbName);
		this.entitymanager = emfactory.createEntityManager();
		
	}
	
	public void describeEntity() {
		
		entities = collectEntities();
		if (entities != null) {
			filteredEntities = filterEntities();
			if (filteredEntities != null) {
				Map<String, List<T>> partitions = partitionEntities();
				if (partitions != null) {
					for (String partitionValue: partitions.keySet()) {
						List<T> partitionEntities = partitions.get(partitionValue);
						PartitionStatistics<T> partitionStats = describeEntityPartition(partitionValue, partitionEntities);
						entityPartitionStats.put(partitionValue, partitionStats);
					}
				} else {
					System.out.println("Entity partitiong failed.");
				}
			} else {
				System.out.println("Entity filtering failed.");
			}
		} else {
			System.out.println("No " + entityTarget + " entities found.");
		}
		
	}
	
	private List<T> collectEntities() {
		
		System.out.println("Starting retriving " + entityTarget + " entities...");
		
		entitymanager.getTransaction().begin();		
		Query query = entitymanager.createQuery("Select e FROM " + entityTarget + " e");
		List<T> entities = query.getResultList();
		entitymanager.getTransaction().commit();
		System.out.println("Retrieved " + entities.size() + " entities.");

		return entities;
	}
	
	private List<T> filterEntities() {
		System.out.println("Starting entity filtering...");
		EntityFiltering<T> entityFiltering = new EntityFiltering<>(entities);
		List<T> filteredEntities = null;;
		if (filterAttribute == null && filterAttributeCount == null) {
			filteredEntities = entityFiltering.identityFilter();
		}
		if (filterAttribute != null) {
			if (filterSubAttribute == null) {
			filteredEntities = entityFiltering.filterByAttrValue(filterAttribute, filterValue);
			} else {
				filteredEntities = entityFiltering.filterByAttrValueInList(filterAttribute, filterSubAttribute, filterValue);
			}
		}		
		if (filterAttributeCount != null) {
			System.out.println("Filter by count");
			filteredEntities = entityFiltering.filterByNumElementsInList(filterAttributeCount, countOperator, filterCountValue);
		}
		System.out.println("Entity filtering completed.");
		return filteredEntities;
	}
	
	private Map<String, List<T>> partitionEntities() {
		System.out.println("Starting entity partitioning...");
		EntityPartitioning<T> entityPartitioning = new EntityPartitioning<>(filteredEntities);
		Map<String, List<T>>  partitions = null;
		if (partitionSubDimension == null) {
			partitions = entityPartitioning.partitionByAttrValue(partitionDimension);
		} else {
			partitions = entityPartitioning.partitionByAttrValueInList(partitionDimension, partitionSubDimension);
		}
		System.out.println("Entity partitioning completed.");
		return partitions;
	}
	
	private PartitionStatistics<T> describeEntityPartition(String partitionValue, List<T> partitionEntities) {
		System.out.println("Starting computing statistics in partition identified by value " + partitionValue + "...");
		PartitionStatistics<T> partitionStats = new PartitionStatistics<>(partitionEntities, statsAttributes);
		partitionStats.countEntities();
		partitionStats.countMissingValuesInAttributes();
		System.out.println("Statistics computation completed.");
		return partitionStats;				
	}
	
	public Map<String, PartitionStatistics<T>> getEntityPartitionStatistics() {
		return entityPartitionStats;
	}
	
	public Map<String, Map<String, Integer>> getMissingValueFreq() {
		Map<String, Map<String, Integer>> attrMissingValueFreqMap = new HashMap<>();
		
		for (String partitionValue: entityPartitionStats.keySet()) {
			PartitionStatistics<T> partitionStats = entityPartitionStats.get(partitionValue);

			Map<String, Integer> attrMissingValueFreq = partitionStats.getAttrMissingValueFreq();
			attrMissingValueFreqMap.put(partitionValue, attrMissingValueFreq);			
		}
		return attrMissingValueFreqMap;
	}
	
	public Map<String, Map<String, Float>> getMissingValueFreqPercentage() {
		Map<String, Map<String, Float>> attrMissingValueFreqPercentageMap = new HashMap<>();
		
		for (String partitionValue: entityPartitionStats.keySet()) {
			PartitionStatistics<T> partitionStats = entityPartitionStats.get(partitionValue);
			Map<String, Float> attrMissingValueFreqPercentage = partitionStats.getAttrMissingValueFreqPercentage();
			attrMissingValueFreqPercentageMap.put(partitionValue, attrMissingValueFreqPercentage);			
		}
		return attrMissingValueFreqPercentageMap;
	}
	
	public Map<String, Integer> getCount() {
		Map<String, Integer> countMap = new HashMap<>();	
		
		for (String partitionValue: entityPartitionStats.keySet()) {
			PartitionStatistics<T> partitionStats = entityPartitionStats.get(partitionValue);
			countMap.put(partitionValue, partitionStats.getEntitiesCount());			
		}
		
		return countMap;
	}
	
	public void exportStatistics(String filename, List<String> filterAttributes) {
		
		System.out.println("Starting exporting statistics...");
		
		Map<String, Integer> countMap = getCount();	
		Map<String, Map<String, Integer>> attrMissingValueFreqMap = getMissingValueFreq();
		Map<String, Map<String, Float>> attrMissingValuePercentageFreqMap = getMissingValueFreqPercentage();
		
		List<List<String>> stats = new ArrayList<List<String>>();
			
		List<String> header = new ArrayList<>();
		header.add("");
		header.add("Count");		
		if (filterAttributes != null) {
			header.addAll(filterAttributes);
		} else {					
			
			List<String> partitionsValues = new ArrayList<>(attrMissingValueFreqMap.keySet());
			String firstPartitionValue = partitionsValues.get(0);
			List<String> attributes = new ArrayList<>(attrMissingValueFreqMap.get(firstPartitionValue).keySet());
			header.addAll(attributes);
		}				
		
		
		stats.add(header);
		
		for(String partitionValue: countMap.keySet()) {					
		
			List<String> row = new ArrayList<String>();
			
			row.add(partitionValue);
			
			row.add(""+countMap.get(partitionValue));
			
			Map<String, Integer> absoluteMissingValue = attrMissingValueFreqMap.get(partitionValue);
			Map<String, Float> percentageMissingValue = attrMissingValuePercentageFreqMap.get(partitionValue);
			
			List<String> exportAttrs = null;
			if (filterAttributes != null) {
				exportAttrs = filterAttributes;
			} else {
				exportAttrs = new ArrayList<>(absoluteMissingValue.keySet());
			}
			
			for(String attr: exportAttrs) {								
								
				//String value = "" + absoluteMissingValue.get(attr) + " (" + String.format("%.02f", percentageMissingValue.get(attr)) + ")"; 
				String value = String.format("%.02f", percentageMissingValue.get(attr));
				row.add(value);
			}
			
			stats.add(row);
			
		}
		
		CSVFileWriter writer = new CSVFileWriter(filename);
        writer.write(stats);

        System.out.println("Statistics exporting successfully completed.");
	}
	
	public void displayStatistics() {		
		
		Map<String, Integer> countMap = getCount();	
		Map<String, Map<String, Integer>> attrMissingValueFreqMap = getMissingValueFreq();
		Map<String, Map<String, Float>> attrMissingValuePercentageFreqMap = getMissingValueFreqPercentage();		
		
		for(String partitionValue: countMap.keySet()) {		
			
			System.out.println("\t" + partitionValue);
			
			System.out.println("\t\tCount = " + countMap.get(partitionValue));
			
			Map<String, Integer> absoluteMissingValue = attrMissingValueFreqMap.get(partitionValue);
			Map<String, Float> percentageMissingValue = attrMissingValuePercentageFreqMap.get(partitionValue);			
			
			System.out.println("\t\tMissing values");			
			for(String attr: absoluteMissingValue.keySet()) {								
								
				String value = "" + absoluteMissingValue.get(attr) + " (" + percentageMissingValue.get(attr) + ")"; 
				System.out.println("\t\t\t" + attr + " = " + value);
			}
			
		}
	}
}
