package it.unimore.alps.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.Source;

public class StatsUtil {

	public static void main(String[] args) {
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory("alps");
		EntityManager entitymanager = emfactory.createEntityManager();

		entitymanager.getTransaction().begin();
		Query query = entitymanager.createQuery("Select o FROM Organization o");
		List<Organization> organizations = query.getResultList();

		System.out.println("Number of organizations: " + organizations.size());

		execute(organizations);

	}

	public static void execute(List<Organization> organizations) {
		print(groupBySource(organizations));
	}

	public static void print(Map<String, List<Organization>> sourceOrganizations) {
		for (Map.Entry<String, List<Organization>> entry : sourceOrganizations.entrySet()) {
			System.out.println("SOURCE: " + entry.getKey());
			printStats(entry.getValue());
		}
	}

	private static void printStats(List<Organization> orgs) {

		Set<String> keys = new HashSet<>();
		keys.addAll(orgs.get(0).retrieveValues().keySet());

		Map<String, Integer> valueNotNullCount = new HashMap<>();

		for (Organization org : orgs) {
			for (String key : keys) {

				if (org.retrieveValues().get(key) != null) {
					// String value = org.retrieveValues().get(key);
					if (!valueNotNullCount.containsKey(key)) {
						valueNotNullCount.put(key, 1);
					} else {
						int c = valueNotNullCount.get(key);
						c = c + 1;
						valueNotNullCount.remove(key);
						valueNotNullCount.put(key, c);

					}
				}
			}

		}

		for (Map.Entry<String, Integer> valueCount : valueNotNullCount.entrySet()) {
			System.out.println("\t" + valueCount.getKey() + "\t" + (valueCount.getValue() / (orgs.size() + 0.0d)));
		}

	}

	public static Map<String, List<Organization>> groupBySource(List<Organization> organizations) {

		Map<String, List<Organization>> source_organizations = new HashMap<>();

		for (Organization org : organizations) {
			for (Source s : org.getSources()) {
				String name = s.getLabel();

				if (source_organizations.containsKey(name)) {
					source_organizations.get(name).add(org);
				} else {
					List<Organization> orgs = new ArrayList<>();
					orgs.add(org);
					source_organizations.put(name, orgs);
				}
			}
		}

		System.out.println("Number of sources: " + source_organizations.size());
		return source_organizations;

	}

}
