package it.unimore.alps.statistics;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EntityPartitioning<T> {

	private List<T> entities;
	
	public EntityPartitioning(List<T> entities) {
		this.entities = entities;
	}
	
	
	public Map<String, List<T>> partitionByAttrValue(String attr) {
		
		Map<String, List<T>> partitionsMap = new HashMap<>();
		
		T firstEntity = entities.get(0);
		
		System.out.println("Starting partitioning " + firstEntity.getClass().getSimpleName() + " entities by attribute " + attr + "...");
		
		if (checkIfAttributeBelongToEntity(firstEntity, attr)) {
			for (T entity: entities) {
				try {
					String attributeVal = (String) getEntityAttributeValue(entity, attr);
					List<T> partitionEntities = new ArrayList<>();
					if (partitionsMap.containsKey(attributeVal)) {
						partitionEntities = partitionsMap.get(attributeVal);
					}
					partitionEntities.add(entity);
					partitionsMap.put(attributeVal, partitionEntities);
				} catch (NoSuchFieldException | SecurityException e) {
					e.printStackTrace();
					return partitionsMap;
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
					return partitionsMap;
				} catch (IllegalAccessException e) {
					e.printStackTrace();
					return partitionsMap;
				}								
			}
		} else {
			System.out.println("Attribute " + attr + " doesn't appartain to " + firstEntity.getClass().getSimpleName() + " entity type.");
		}
		
		System.out.println("Partitioning by attribute completed.");
		
		return partitionsMap;
	}	
	
	private Object getEntityAttributeValue(Object entity, String attr) throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		
		Class<?> entityClass = entity.getClass();
		Field f = entityClass.getDeclaredField(attr);
		f.setAccessible(true);
		Object value = (Object) f.get(entity);								
		
		return value;
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
	
	private boolean checkIfAttributeBelongToEntityInList(List<T> entities, String listAttr, String attr) {

		for (T entity: entities) {			
			try {

				List<Object> list = (List<Object>) getEntityAttributeValue(entity, listAttr);
				if (list != null && list.size() > 0) {
					Object listItem = list.get(0);
					return checkIfAttributeBelongToEntity(listItem, attr);
				}
				
			} catch (NoSuchFieldException | SecurityException e) {
				return false;
			} catch (IllegalArgumentException e) {
				return false;
			} catch (IllegalAccessException e) {
				return false;
			}
		}
		return false;
	}
	
	
	public Map<String, List<T>> partitionByAttrValueInList(String listAttr, String attr) {
		
		Map<String, List<T>> partitionsMap = new HashMap<>();
		
		T firstEntity = entities.get(0);
		
		System.out.println("Starting partitioning " + firstEntity.getClass().getSimpleName() + " entities by list attribute " + listAttr + "...");
		
		if (checkIfAttributeBelongToEntity(firstEntity, listAttr)) {
			
			if (checkIfAttributeBelongToEntityInList(entities, listAttr, attr)) {
			
				for (T entity: entities) {
					try {
						List<Object> list = (List<Object>) getEntityAttributeValue(entity, listAttr);
						for(Object item: list) {
							String attributeVal = (String) getEntityAttributeValue(item, attr);
							List<T> partitionEntities = new ArrayList<>();
							if (partitionsMap.containsKey(attributeVal)) {
								partitionEntities = partitionsMap.get(attributeVal);
							}
							partitionEntities.add(entity);
							partitionsMap.put(attributeVal, partitionEntities);
						}						
					} catch (NoSuchFieldException | SecurityException e) {
						e.printStackTrace();
						return partitionsMap;
					} catch (IllegalArgumentException e) {
						e.printStackTrace();
						return partitionsMap;
					} catch (IllegalAccessException e) {
						e.printStackTrace();
						return partitionsMap;
					}								
				}
			} else {
				System.out.println("Sub-attribute " + attr + " doesn't appartain to " + listAttr +" attribute in " + firstEntity.getClass().getSimpleName() + " entity type.");
			}
			
		} else {
			System.out.println("Attribute " + attr + " doesn't appartain to " + firstEntity.getClass().getSimpleName() + " entity type.");
		}
		
		System.out.println("Partitioning by attribute completed.");
		
		return partitionsMap;
	}
	
	public Map<String, List<T>> identityPartition() {
		System.out.println("Identity partitioning");
		Map<String, List<T>> partitionsMap = new HashMap<>();
		partitionsMap.put("0", entities);
		return partitionsMap;
	}
	
	
}
