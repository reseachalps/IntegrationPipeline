package it.unimore.alps.statistics;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PartitionStatistics<T> {
	
	private List<T> entities;
	private int entitiesCount;
	private Map<String, Integer> attrMissingValueFreq;
	private Map<String, Float> attrMissingValueFreqPercentage;
	private List<String> statsAttributes;
	
	public PartitionStatistics(List<T> entities, List<String> statsAttributes) {
		this.entities = entities;
		this.statsAttributes = statsAttributes;
	}
	
	public void countEntities() {
		System.out.println("Counting entities...");
		this.entitiesCount = entities.size();
	}
	
	public void countMissingValuesInAttributes() {
		Map<String, Integer> attrMissingValueFreq = new HashMap<>();
		
		T firstEntity = entities.get(0);
		System.out.println("Starting computing missing value frequency in " + firstEntity.getClass().getSimpleName() + " entities...");
		
		Class<?> entityClass = firstEntity.getClass();
		Field[] fields = entityClass.getDeclaredFields();

        for (Field field : fields) {
        	field.setAccessible(true);
			String attribute = field.getName().toString();
			
			if (!statsAttributes.contains(attribute)) {
				continue;
			}
			
			attrMissingValueFreq.put(attribute, 0);
			for (T entity: entities) {
				try {
					boolean isNull = false;
					try {
					
						List<?> list = (List<?>) field.get(entity);
						if (list.size() == 0) {
							isNull = true;
						}
						
					} catch (Exception e) {
						
						if (field.get(entity) == null) {
							isNull = true;
						}
						
					}										
					
					if (isNull) {
						if (attrMissingValueFreq.containsKey(attribute)) {
							int freqMissingValues = attrMissingValueFreq.get(attribute);
							attrMissingValueFreq.put(attribute, freqMissingValues+1);
						} else {
							attrMissingValueFreq.put(attribute, 1);
						}
					}
				} catch (IllegalArgumentException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}				
			}

		}
        
        if (entitiesCount == 0) {
        	countEntities();
        }
        Map<String, Float> attrMissingValueFreqPercentage = new HashMap<>();
        for(String attr: attrMissingValueFreq.keySet()) {
        	attrMissingValueFreqPercentage.put(attr, (attrMissingValueFreq.get(attr) / (float) entitiesCount)*100);
        }
        
        System.out.println("Missing value frequency computation completed.");
		
		this.attrMissingValueFreq = attrMissingValueFreq;
		this.attrMissingValueFreqPercentage = attrMissingValueFreqPercentage;
	}	
	
	public int getEntitiesCount() {
		return entitiesCount;
	}
	
	public Map<String, Integer> getAttrMissingValueFreq() {
		return attrMissingValueFreq;
	}
	
	public Map<String, Float> getAttrMissingValueFreqPercentage() {
		return attrMissingValueFreqPercentage;
	}

}
