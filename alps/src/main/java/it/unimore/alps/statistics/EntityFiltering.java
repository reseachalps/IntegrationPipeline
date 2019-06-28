package it.unimore.alps.statistics;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EntityFiltering<T> {

	private List<T> entities;
	
	public EntityFiltering(List<T> entities) {
		this.entities = entities;
	}
	
	public List<T> identityFilter() {
		System.out.println("Identity filter");
		return entities;
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
	
	public List<T> filterByAttrValue(String attr, String value) {
		
		boolean isNull = false;
		if (value.equals("null")) {
			isNull = true;
		}
		
		List<T> filteredEntities = new ArrayList<>();
		
		// check if the input attribute exists for the considered entity type
		T firstEntity = entities.get(0);
		
		System.out.println("Starting filtering " + firstEntity.getClass().getSimpleName() + " entities by value " + value + " in " + attr + " attribute...");
		
		if (checkIfAttributeBelongToEntity(firstEntity, attr)) {
			for (T entity: entities) {
				
				try {
					Object attributeValObject = (Object) getEntityAttributeValue(entity, attr);
					if (isNull) {
						if (attributeValObject == null) {
							filteredEntities.add(entity);
						}
					} else {
						String attributeVal = (String) attributeValObject;
						if (attributeVal.equals(value)) {
							filteredEntities.add(entity);
						}
					}
				} catch (NoSuchFieldException | SecurityException e) {
					e.printStackTrace();
					return new ArrayList<>();
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
					return new ArrayList<>();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
					return new ArrayList<>();
				}				
				
			}
		} else {
			System.out.println("Attribute " + attr + " doesn't appartain to " + firstEntity.getClass().getSimpleName() + " entity type.");
		}
		
		System.out.println("Filtering by attribute value completed.");
		
		this.entities = filteredEntities;
		
		return filteredEntities;
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
	
	
	public List<T> filterByAttrValueInList(String listAttr, String attr, String value) {
		
		List<T> filteredEntities = new ArrayList<>();
		
		T firstEntity = entities.get(0);
		
		System.out.println("Starting filtering " + firstEntity.getClass().getSimpleName() + " entities by value " + value + " in " + attr + " list attribute...");
		
		if (checkIfAttributeBelongToEntity(firstEntity, listAttr)) {
			
			if (checkIfAttributeBelongToEntityInList(entities, listAttr, attr)) {
			
				for (T entity: entities) {
					try {
						List<Object> list = (List<Object>) getEntityAttributeValue(entity, listAttr);
						boolean found = false;
						for(Object item: list) {
							String attributeVal = (String) getEntityAttributeValue(item, attr);
							if (attributeVal.equals(value)) {
								found = true;
							}
						}						
						if (found) {
							filteredEntities.add(entity);
						}
					} catch (NoSuchFieldException | SecurityException e) {
						e.printStackTrace();
						return new ArrayList<>();
					} catch (IllegalArgumentException e) {
						e.printStackTrace();
						return new ArrayList<>();
					} catch (IllegalAccessException e) {
						e.printStackTrace();
						return new ArrayList<>();
					}								
				}
			} else {
				System.out.println("Sub-attribute " + attr + " doesn't appartain to " + listAttr +" attribute in " + firstEntity.getClass().getSimpleName() + " entity type.");
			}
			
		} else {
			System.out.println("Attribute " + attr + " doesn't appartain to " + firstEntity.getClass().getSimpleName() + " entity type.");
		}
		
		System.out.println("Filtering by attribute value completed.");
		
		this.entities = filteredEntities;
		
		return filteredEntities;
	}
	
	public List<T> filterByNumElementsInList(String listName, String filteringType, int numThreshold) {
		
		List<T> filteredEntities = new ArrayList<>();
		
		// check if the input attribute exists for the considered entity type
		T firstEntity = entities.get(0);
		
		System.out.println("Starting count filtering " + firstEntity.getClass().getSimpleName() + " entities by count value " + numThreshold + " in " + listName + " list attribute with " + filteringType +" operator...");
		
		if (checkIfAttributeBelongToEntity(firstEntity, listName)) {
			for (T entity: entities) {
				try {
					List<Object> list = (List<Object>) getEntityAttributeValue(entity, listName);
					if (list != null) {
					
						if (filteringType.equals("equality")) {
							if (list.size() == numThreshold) {
								filteredEntities.add(entity);
							}
						}
						if (filteringType.equals("greater")) {
							if (list.size() > numThreshold) {
								filteredEntities.add(entity);
							}
						}
						if (filteringType.equals("lower")) {
							if (list.size() < numThreshold) {
								filteredEntities.add(entity);
							}
						}
					}
				} catch (NoSuchFieldException | SecurityException e) {
					e.printStackTrace();
					return new ArrayList<>();
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
					return new ArrayList<>();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
					return new ArrayList<>();
				}
				
				
			}
		} else {
			System.out.println("List " + listName + " doesn't appartain to " + firstEntity.getClass().getSimpleName() + " entity type.");
		}
		
		System.out.println("Filtering by count value completed.");
		
		this.entities = filteredEntities;
		
		return filteredEntities;
	}
}
