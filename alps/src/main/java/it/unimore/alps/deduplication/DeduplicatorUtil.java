package it.unimore.alps.deduplication;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import it.unimore.alps.sql.model.Badge;
import it.unimore.alps.sql.model.Leader;
import it.unimore.alps.sql.model.Link;
import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationActivity;
import it.unimore.alps.sql.model.Person;
import it.unimore.alps.sql.model.Project;
import it.unimore.alps.sql.model.Publication;
import it.unimore.alps.sql.model.Source;

public class DeduplicatorUtil {

	public static void main(String[] args) {

		CommandLine commandLine;
		Option correspondenceOptionOrgs = Option.builder("correpondenceFileOrgs").hasArg().required(true)
				.desc("The file that contains organization correspondence. ").longOpt("correpondenceFileOrgs").build();
		Option correspondenceOptionPrjs = Option.builder("correpondenceFilePrjs").hasArg().required(true)
				.desc("The file that contains project correspondence. ").longOpt("correpondenceFilePrjs").build();
		Options options = new Options();

		options.addOption(correspondenceOptionOrgs);
		options.addOption(correspondenceOptionPrjs);

		CommandLineParser parser = new DefaultParser();

		String fileCorrespondenceOrgs = null;
		String fileCorrespondencePrjs = null;
		try {
			commandLine = parser.parse(options, args);

			if (commandLine.hasOption("correpondenceFileOrgs")) {
				fileCorrespondenceOrgs = commandLine.getOptionValue("correpondenceFileOrgs");
			} else {
				System.out.println("Organization correspondences folder not provided. Use the orgFolder option.");
				System.exit(0);
			}

			System.out.println("Org file: " + fileCorrespondenceOrgs);
			if (commandLine.hasOption("correpondenceFilePrjs")) {
				fileCorrespondencePrjs = commandLine.getOptionValue("correpondenceFilePrjs");
			} else {
				System.out.println("Project correspondences folder not provided. Use the orgFolder option.");
				System.exit(0);
			}
			System.out.println("Prjs file: " + fileCorrespondencePrjs);

		} catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		DeduplicatorUtil dutil = new DeduplicatorUtil();

		// Set<Set<String>> correspondeces = dutil
		// .readCorrespondences("/Users/paolosottovia/Downloads/researchAlpsCsvs/correspondence.tsv");

		Set<Set<String>> correspondecesOrgs = dutil.readCorrespondences(fileCorrespondenceOrgs);
		Set<Set<String>> correspondecesPrjs = dutil.readCorrespondences(fileCorrespondencePrjs);

		System.out.println("Number of organization correspondences: " + correspondecesOrgs.size());
		List<Organization> organizations = dutil.retrieveOrganizations();
		Set<String> allIds = new HashSet<>();
		allIds.addAll(dutil.retrieveAllOrganizationIds(organizations));

		Set<String> allProjectIds = dutil.retrieveAllProjectIds(organizations);

		Map<String, Project> idProjects = dutil.retriveProjectMap(organizations);

		Map<String, String> resolvedProject = dutil.resolveProjects(allProjectIds, correspondecesPrjs);

		Set<String> remainingIds = new HashSet<>();
		remainingIds.addAll(allIds);

		Set<String> corresponceIds = new HashSet<>();

		for (Set<String> corr : correspondecesOrgs) {
			for (String c : corr) {
				remainingIds.remove(c);
				if (corresponceIds.contains(c)) {
					System.err.println("DUPLICATE: " + c);
				} else {
					corresponceIds.add(c);
				}
			}
		}

		System.out.println("Number of organization ids : " + allIds.size());
		System.out.println("Number of organization ids without duplicates : " + remainingIds.size());

		Map<String, Organization> idOrganizations = dutil.getOrganizationMap(organizations);

		List<Organization> organizationDeduplicated = new ArrayList<>();
		for (String id : remainingIds) {
			organizationDeduplicated.add(idOrganizations.get(id));
		}
		System.out.println("Organization non deduplicated: " + organizationDeduplicated.size());
		List<Organization> organizationDeduplicatedComplexCases = new ArrayList<>();

		int i = 0;

		for (Set<String> components : correspondecesOrgs) {
			List<Organization> orgs = new ArrayList<>();
			if (i % 25 == 0) {
				System.out.println("Progress: " + i);
			}

			// if (i > 10) {
			// break;
			// }
			for (String c : components) {
				orgs.add(idOrganizations.get(c));
			}

			for (Organization org : orgs) {

				for (OrganizationActivity act : org.getActivities()) {
					System.out.println("\t\t" + act.toString());

					// if (act.getLabel().equals("Exobiologie et astrochimie")) {
					// System.out.println("OTHER \t ORG: " + org.getLabel() + "\t" + act.getCode());
					// System.exit(-1);
					// }
				}

			}

			Organization orgCompressed = dutil.compressDuplicatesOrganizations(orgs, resolvedProject, idProjects);
			organizationDeduplicatedComplexCases.add(orgCompressed);
			i++;
		}
		System.out.println("Number of organization to compressed: " + organizationDeduplicatedComplexCases.size());
		// try to insert new

		System.out.println("===================================================================================");
		for (Organization org : organizationDeduplicatedComplexCases) {

			for (OrganizationActivity act : org.getActivities()) {
				System.out.println("\t\t" + act.toString());
			}

		}

		// search for specific label

		for (Organization org : organizationDeduplicated) {

			for (OrganizationActivity act : org.getActivities()) {
				if (act.getLabel().equals("Exobiologie et astrochimie")) {
					System.out.println("NON DEDUP \t ORG: " + org.getLabel() + "\t" + act.getCode());
				}
			}

		}

		// for (Organization org : organizationDeduplicatedComplexCases) {
		//
		// for (OrganizationActivity act : org.getActivities()) {
		// if (act.getLabel().equals("Exobiologie et astrochimie")) {
		// System.out.println("COMPLEX \t ORG: " + org.getLabel() + "\t" +
		// act.getCode());
		// }
		// }
		//
		// }

		System.out.println("LABELS");
		for (Organization org : organizationDeduplicatedComplexCases) {
			System.out.println("\t" + org.getLabel());
		}
		System.out.println("END LABELS");

		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory("alpsDedup");

		EntityManager entitymanager = emfactory.createEntityManager();

		System.out.println("Number of organization not deduplicated: " + organizationDeduplicated.size());
		entitymanager.getTransaction().begin();
		for (Organization org : organizationDeduplicated) {

			System.out.println("Label: " + org.getLabel());
			insertOrganization(entitymanager, org);

		}
		entitymanager.getTransaction().commit();
		System.out.println("Insertion of complex cases!!!!");
		entitymanager.getTransaction().begin();
		for (Organization org : organizationDeduplicatedComplexCases) {

			insertOrganization(entitymanager, org);

		}
		entitymanager.getTransaction().commit();
		entitymanager.close();
		emfactory.close();

	}

	private static void insertOrganization(EntityManager entitymanager, Organization org) {

		for (Organization orgC : org.getChildrenOrganizations()) {
			insertOrganization(entitymanager, orgC);
		}

		System.out.println(
				"\n\n\n=========================================================================================================================================================");
		// entitymanager.getTransaction().begin();

		System.out.println("LABEL: " + org.getLabel());
		System.out.println("Label: " + org.getLabel());
		System.out.println("SOURCE:");
		for (Source s : org.getSources()) {
			entitymanager.persist(s);
			System.out.println("\t\t" + s.getLabel());
		}
		System.out.println("\nACTIVITY LIST:");
		for (OrganizationActivity act : org.getActivities()) {

			System.out.println("\t\t" + act.toString());
		}

		org.getActivities().clear();

		// for (OrganizationActivity act : org.getActivities()) {
		// if (act.getCode() != null) {
		// // OrganizationActivity orgAct = null;
		// if (((OrganizationActivity) objExists(entitymanager,
		// OrganizationActivity.class,
		// act.getCode())) == null) {
		// // entitymanager.persist(act);
		// // entitymanager.flush();
		// } else {
		// System.err.println("DUPLICATE ACTIVITY CODE: " + act.toString());
		// }
		// }
		// // System.out.println("\t\t" + act.toString());
		// }
		// entitymanager.flush();
		System.out.println("\nPROJECT:");
		List<String> pjs = new ArrayList<>();
		Set<Integer> projectIds = new HashSet<>();
		boolean nullCase = false;

		if (org.getProjects() != null) {

			for (Project p : org.getProjects()) {

				if (p != null) {

					entitymanager.persist(p);
					String pString = "\t\t\t" + p.getId() + "\t" + p.getLabel();
					if (pjs.contains(pString)) {
						System.out.println("DUPLICATE: " + pString);
					} else {
						System.out.println(pString);
					}

					int id = p.getId();
					if (projectIds.contains(id)) {
						System.err.println("Duplicate PROJECT CODE: " + id);
					} else {
						projectIds.add(id);
					}
				} else {
					System.err.println("ORGANIZATION:  \t" + org.getLabel() + "\t has  a NULL PROJECT!!!! CASE 1");
					nullCase = true;
				}

			}

			if (nullCase) {
				if (org.getProjects().size() == 1) {

					org.getProjects().clear();
				}

				List<Project> projectsUpdated = new ArrayList<>();
				for (Project prj : org.getProjects()) {
					if (prj != null) {
						projectsUpdated.add(prj);
					}
				}
				org.setProjects(projectsUpdated);

			}

			List<Publication> publications = new ArrayList<>();

			List<Person> people = new ArrayList<>();
			org.setPublications(publications);
			org.setPeople(people);

		} else {
			System.err.println("ORGANIZATION: \t has NULL PROJECTS!!!! CASE 2");
		}

		// entitymanager.

		// if (objExists(entitymanager, Organization.class, org.getId()) == null) {
		// entitymanager.persist(org);
		// // entitymanager.flush();
		// }
		// entitymanager.flush();
		// entitymanager.getTransaction().commit();

		entitymanager.persist(org);
	}

	private Map<String, String> resolveProjects(Set<String> project, Set<Set<String>> projectCorrespondances) {
		Map<String, String> projectResolutionMap = new HashMap<>();

		for (Set<String> component : projectCorrespondances) {
			String candidateRight = null;
			List<String> comp = new ArrayList<>();
			comp.addAll(component);

			candidateRight = comp.get(0);
			projectResolutionMap.put(candidateRight, candidateRight);
			for (int i = 1; i < comp.size(); i++) {
				projectResolutionMap.put(comp.get(i), candidateRight);
			}

		}

		return projectResolutionMap;
	}

	private Map<String, Project> retriveProjectMap(List<Organization> organizations) {

		Map<String, Project> idProject = new HashMap<>();

		for (Organization o : organizations) {
			for (Project p : o.getProjects()) {
				if (!idProject.containsKey(p.getId() + "")) {
					String id = p.getId() + "";
					idProject.put(id, p);
				}
			}
		}
		return idProject;
	}

	private Set<String> retrieveAllProjectIds(List<Organization> organizations) {
		Set<String> ids = new HashSet<>();
		for (Organization o : organizations) {
			for (Project p : o.getProjects()) {
				String id = p.getId() + "";
				ids.add(id);
			}
		}
		return ids;
	}

	public static Object objExists(EntityManager em, Class entity, String attValue) {

		Object res = em.find(entity, attValue);

		return res;

	}

	public static Object objExists(EntityManager em, Class entity, Integer attValue) {

		Object res = em.find(entity, attValue);

		return res;

	}

	private Organization compressDuplicatesOrganizations(List<Organization> orgs,
			Map<String, String> projectResolutionMap, Map<String, Project> idProjects) {

		Organization cand = orgs.get(0);
		Set<Integer> childrenOrganizationIds = new HashSet<>();
		Set<Integer> badgeIds = new HashSet<>();
		Set<Integer> leaderIds = new HashSet<>();
		Set<Integer> peopleIds = new HashSet<>();
		Set<Integer> publicationIds = new HashSet<>();
		Set<String> activityCodes = new HashSet<>();

		Set<Integer> linksIds = new HashSet<>();
		Set<String> projectsIds = new HashSet<>();

		Set<OrganizationActivity> listMerda = new HashSet<>();
		listMerda.addAll(cand.getActivities());

		System.out.println("CAND: " + cand.getLabel());
		for (Source s : cand.getSources()) {
			System.out.println("\t" + s.getLabel());
		}
		for (int i = 1; i < orgs.size(); i++) {

			Organization item = orgs.get(i);
			cand.getOrganizationExtraFields().addAll(item.getOrganizationExtraFields());
			cand.getOrganizationIdentifiers().addAll(item.getOrganizationIdentifiers());
			cand.getOrganizationRelations().addAll(item.getOrganizationRelations());

			// cand.getChildrenOrganizations().addAll(item.getChildrenOrganizations());

			for (Organization org : cand.getChildrenOrganizations()) {
				childrenOrganizationIds.add(org.getId());
			}

			for (Organization child : cand.getChildrenOrganizations()) {
				if (!childrenOrganizationIds.contains(child.getId())) {
					childrenOrganizationIds.add(child.getId());
					cand.getChildrenOrganizations().add(child);
				}
			}

			for (OrganizationActivity oa : cand.getActivities()) {
				activityCodes.add(oa.getCode());

			}

			for (OrganizationActivity act : item.getActivities()) {

				if (!activityCodes.contains(act.getCode())) {
					activityCodes.add(act.getCode());
					cand.getActivities().add(act);
				}
			}

			// only for debug
			listMerda.addAll(item.getActivities());

			// cand.getBadges().addAll(item.getBadges());

			for (Badge badge : cand.getBadges()) {
				badgeIds.add(badge.getId());
			}

			for (Badge b : item.getBadges()) {
				if (!badgeIds.contains(b.getId())) {
					badgeIds.add(b.getId());
					cand.getBadges().add(b);
				}
			}

			// cand.getLeaders().addAll(item.getLeaders());

			for (Leader l : cand.getLeaders()) {
				leaderIds.add(l.getId());
			}

			for (Leader l : item.getLeaders()) {
				if (!leaderIds.contains(l.getId())) {
					leaderIds.add(l.getId());
					cand.getLeaders().add(l);
				}
			}

			for (Person p : cand.getPeople()) {
				peopleIds.add(p.getId());
			}

			for (Person p : item.getPeople()) {
				if (!peopleIds.contains(p.getId())) {
					peopleIds.add(p.getId());
					cand.getPeople().add(p);
				}
			}

			for (Publication p : cand.getPublications()) {
				publicationIds.add(p.getId());
			}

			for (Publication p : item.getPublications()) {
				if (!publicationIds.contains(p.getId())) {
					publicationIds.add(p.getId());
					cand.getPublications().add(p);
				}
			}

			// cand.getLinks().addAll(item.getLinks());

			for (Link link : cand.getLinks()) {
				linksIds.add(link.getId());
			}

			for (Link link : item.getLinks()) {

				if (!linksIds.contains(link.getId())) {
					linksIds.add(link.getId());
					cand.getLinks().add(link);
				}

			}

			// cand.getProjects().addAll(item.getProjects());
			if (cand.getProjects() != null) {
				for (Project project : cand.getProjects()) {
					if (project != null) {
						String id = project.getId() + "";
						projectsIds.add(projectResolutionMap.get(id));
					}
				}
			}

			if (item.getProjects() != null) {
				for (Project project : item.getProjects()) {
					if (!projectsIds.contains(projectResolutionMap.get(project.getId() + ""))) {
						String id = projectResolutionMap.get(project.getId() + "");
						projectsIds.add(id);
						Project resolvedProject = idProjects.get(id);
						cand.getProjects().add(resolvedProject);
					}

				}
			}

			for (Source s : item.getSources()) {
				if (!cand.getSources().contains(s)) {
					cand.getSources().add(s);
				}
			}

//			if (cand.getAcronym() == null) {
//				if (item.getAcronym() != null) {
//					if (!item.getAcronym().equals("") && !item.getAcronym().toLowerCase().equals("null")) {
//						cand.setAcronym(item.getAcronym());
//					}
//				}
//
//			} else if (cand.getAcronym().equals("") || cand.getAcronym().toLowerCase().equals("null")) {
//				if (item.getAcronym() != null) {
//					if (!item.getAcronym().equals("") && !item.getAcronym().toLowerCase().equals("null")) {
//						cand.setAcronym(item.getAcronym());
//					}
//				}
//			}

			// cand.getAddress();

			if (cand.getAddress() == null) {
				if (item.getAddress() != null) {
					if (!item.getAddress().equals("") && !item.getAddress().toLowerCase().equals("null")) {
						cand.setAddress(item.getAddress());
					}
				}

			} else if (cand.getAddress().equals("") || cand.getAddress().toLowerCase().equals("null")) {
				if (item.getAddress() != null) {
					if (!item.getAddress().equals("") && !item.getAddress().toLowerCase().equals("null")) {
						cand.setAddress(item.getAddress());
					}
				}
			}

			// cand.getCity();

			if (cand.getCity() == null) {
				if (item.getCity() != null) {
					if (!item.getCity().equals("") && !item.getCity().toLowerCase().equals("null")) {
						cand.setCity(item.getCity());
					}
				}

			} else if (cand.getCity().equals("") || cand.getCity().toLowerCase().equals("null")) {
				if (item.getCity() != null) {
					if (!item.getCity().equals("") && !item.getCity().toLowerCase().equals("null")) {
						cand.setCity(item.getCity());
					}
				}
			}

			// cand.getPostcode();
			if (cand.getPostcode() == null) {
				if (item.getPostcode() != null) {
					if (!item.getPostcode().equals("") && !item.getPostcode().toLowerCase().equals("null")) {
//						cand.setPostCode(item.getPostcode());
					}
				}

			} else if (cand.getPostcode().equals("") || cand.getPostcode().toLowerCase().equals("null")) {
				if (item.getPostcode() != null) {
					if (!item.getPostcode().equals("") && !item.getPostcode().toLowerCase().equals("null")) {
//						cand.setAcronym(item.getPostcode());
					}
				}
			}

		}

		// Set<OrganizationActivity> activitiesSet = new HashSet<>();
		// activitiesSet.addAll(cand.getActivities());
		// if (activitiesSet.size() != cand.getActivities().size()) {
		//
		// System.out.println("Number of activities LIST: " +
		// cand.getActivities().size());
		// System.out.println("Number of activities SET: " + activitiesSet.size());
		// }

		if (listMerda.size() > 0) {
			System.out.println("--------------------------------------");
			for (OrganizationActivity act : listMerda) {
				System.out.println("\t\t\t" + act.toString());
			}
			System.out.println("--------------------------------------");
		}

		if (cand.getActivities().size() > 0) {
			System.out.println("LABEL: " + cand.getLabel());

			System.out.println("LIST OF ODACTS: ");
			for (OrganizationActivity act : cand.getActivities()) {
				System.out.println("\t\t\t" + act.toString());
			}
		}
		return cand;
	}

	public DeduplicatorUtil() {
		// TODO Auto-generated constructor stub
	}

	public Map<String, Organization> getOrganizationMap(List<Organization> organizations) {
		Map<String, Organization> orgMap = new HashMap<>();
		for (Organization org : organizations) {
			orgMap.put(org.getId() + "", org);
		}
		return orgMap;
	}

	public Set<String> retrieveAllOrganizationIds(List<Organization> organizations) {
		Set<String> ids = new HashSet<>();
		for (Organization org : organizations) {
			ids.add(org.getId() + "");
		}
		return ids;
	}

	public List<Organization> retrieveOrganizations() {
		List<Organization> organizationsAll = new ArrayList<>();

		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory("alps");
		EntityManager entitymanager = emfactory.createEntityManager();

		entitymanager.getTransaction().begin();
		Query query = entitymanager.createQuery("Select o FROM Organization o");
		List<Organization> organizations = query.getResultList();

		organizationsAll.addAll(organizations);

		System.out.println("Number of organizations from database 1: " + organizations.size());

		entitymanager.close();
		emfactory.close();

		// EntityManagerFactory emfactory1 =
		// Persistence.createEntityManagerFactory("scanr");
		// EntityManager entitymanager1 = emfactory1.createEntityManager();
		//
		// entitymanager1.getTransaction().begin();
		// Query query1 = entitymanager1.createQuery("Select o FROM Organization o");
		// List<Organization> organizations1 = query1.getResultList();
		//
		// organizationsAll.addAll(organizations1);
		//
		// System.out.println("Number of organizations from database 2: " +
		// organizations1.size());
		//
		// entitymanager1.close();
		// emfactory1.close();

		System.out.println("Number of all organizations: " + organizationsAll.size());
		return organizationsAll;
	}

	public Set<Set<String>> readCorrespondences(String path) {

		Set<Set<String>> correspondes = new HashSet<>();

		File fileDir = new File(path);

		try {

			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(fileDir), "UTF8"));

			String str;
			int lineCount = 0;
			while ((str = in.readLine()) != null) {

				if (!str.trim().equals("")) {
					String[] items = str.split("\t");
					Set<String> corr = new HashSet<>();
					for (String item : items) {
						corr.add(item);
					}
					if (correspondes.contains(corr)) {
						System.out.println("Duplicate: " + corr.toString());
					}
					correspondes.add(corr);

				}
				lineCount++;
				// System.out.println(str);
			}

			in.close();
			System.out.println("Number of lines: " + lineCount);
		} catch (UnsupportedEncodingException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println(e.getMessage());
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}

		return correspondes;

	}

}
