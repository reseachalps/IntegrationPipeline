package it.unimore.alps.sources.openaire;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import it.unimore.alps.sql.model.Badge;
import it.unimore.alps.sql.model.Leader;
import it.unimore.alps.sql.model.Link;
import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationActivity;
import it.unimore.alps.sql.model.OrganizationExtraField;
import it.unimore.alps.sql.model.OrganizationIdentifier;
import it.unimore.alps.sql.model.OrganizationRelation;
import it.unimore.alps.sql.model.OrganizationType;
import it.unimore.alps.sql.model.Person;
import it.unimore.alps.sql.model.PersonIdentifier;
import it.unimore.alps.sql.model.Project;
import it.unimore.alps.sql.model.ProjectExtraField;
import it.unimore.alps.sql.model.ProjectIdentifier;
import it.unimore.alps.sql.model.Publication;
import it.unimore.alps.sql.model.Source;
import it.unimore.alps.sql.model.Thematic;
import it.unimore.alps.sql.model.Theme;

public class DeleteUtil {

	public static void main(String[] args) {

		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory("alpstesting");
		EntityManager entitymanager = emfactory.createEntityManager();
		entitymanager.getTransaction().begin();
		
		String sourceRevisionDate = "2017-12-15";
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

		Date sourceDate = null;
		try {
			sourceDate = df.parse(sourceRevisionDate);
			// source.setRevisionDate(sourceDate);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Date revisionDate = new Date(2017, 12, 15);
		String sourceLabel = "OpenAire";
		String urlSource = "https://www.openaire.eu/";

		Source source = OpenAireImporter.checkSource(sourceLabel, sourceDate, urlSource, entitymanager);
		//
		Query query = entitymanager.createQuery("SELECT o FROM Organization o WHERE :source MEMBER OF o.sources");
		query.setParameter("source", source);
		// Query query = entitymanager.createQuery("SELECT o FROM Organization o");
		List<Organization> results = query.getResultList();
		boolean debug = true;
		// System.out.println("******************");
		for (Organization o : results) {
			if (debug) {
				System.out.println(o.getLabel());
			}
		}

		System.out.println("Number of organizations: " + results.size());
		entitymanager.getTransaction().commit();
		

		//System.out.println("Is active: "+ entitymanager.getTransaction().isActive());
		entitymanager.getTransaction().begin();
		removeOrganizations(results, entitymanager);
		entitymanager.getTransaction().commit();

		entitymanager.getTransaction().begin();
		entitymanager.remove(source);
		entitymanager.getTransaction().commit();

		entitymanager.close();
		emfactory.close();

	}

	private static void removeOrganizations(List<Organization> orgs, EntityManager entitymanager) {
		
		int numOrgs = 0;
		for (Organization org : orgs) {
			
			if ((numOrgs % 1000) == 0) {
				System.out.println(numOrgs);
			}
			numOrgs++;

			// for (Source s : org.getSources()) {
			// entitymanager.remove(s);
			// }

			// entitymanager.flush();
			
			removeOrganizations(org.getChildrenOrganizations(), entitymanager);
			
			// TODO: check if activities are common with other sources
			// remove activities
			List<OrganizationActivity> acts = org.getActivities();
			org.getActivities().clear();
			for (OrganizationActivity act : acts) {
				if (act != null) {
					entitymanager.remove(act);
				}
			}
			entitymanager.flush();
			
			// remove badges
			List<Badge> badges = org.getBadges();
			org.getBadges().clear();
			for (Badge b : badges) {
				if (b != null) {
					entitymanager.remove(b);
				}
			}			
			entitymanager.flush();
			
			// remove org extra fields
			for (OrganizationExtraField ext : org.getOrganizationExtraFields()) {
				if (ext != null) {
					ext.setOrganization(null);				
					entitymanager.remove(ext);
				}
			}
			entitymanager.flush();			
			
			// remove org identifiers
			for (OrganizationIdentifier id : org.getOrganizationIdentifiers()) {
				if (id != null) {
					id.setOrganization(null);
					entitymanager.remove(id);
				}
			}
			entitymanager.flush();
			
			// remove org people
			List<Person> people = org.getPeople(); 
			org.getPeople().clear();
			for (Person per : people) {
				
				// remove person-source connection
				per.getSources().clear();
				
				// remove person identifiers
				for (PersonIdentifier id : per.getPersonIdentifiers()) {
					if (id != null) {
						entitymanager.remove(id);
					}
				}
				
				entitymanager.remove(per);
			}
			entitymanager.flush();
			
			
			// remove links
			List<Link> links = org.getLinks();
			org.getLinks().clear();
			for (Link l : links) {
				if (l != null) {
					entitymanager.remove(l);
				}
			}			
			entitymanager.flush();
			
			// remove org relations
			for (OrganizationRelation rel : org.getOrganizationRelations()) {
				if (rel != null) {
					rel.setOrganization(null);
					entitymanager.remove(rel);
				}
			}
			entitymanager.flush();
			
			// TODO: check if organization type is common to other sources
			OrganizationType orgType = org.getOrganizationType();
			org.setOrganizationType(null);
			if (orgType != null) {
				entitymanager.remove(orgType);
			}
			entitymanager.flush();

			// remove organization-source connection
			org.getSources().clear();
			entitymanager.flush();
			
			
			
			// TODO: publications and people
			/*for (Publication pub : org.getPublications()) {
				pub.getSources().clear();
				entitymanager.remove(pub);
			}*/
			
			// remove project-organization connection
			List<Project> projects = new ArrayList<>();
			projects.addAll(org.getProjects());
			org.getProjects().clear();
			entitymanager.flush();
			
			// entitymanager.merge(org);
			for (Project p : projects) {
				
				// remove project-source connection
				p.getSources().clear();
				
				// entitymanager.getTransaction().begin();
				// FSystem.out.println("Project: " + p.getId());
				
				// remove project identifiers
				for (ProjectIdentifier id : p.getProjectIdentifiers()) {
					if (id != null) {
						entitymanager.remove(id);
					}
				}
				
				// remove project extra fields
				for (ProjectExtraField id : p.getProjectExtraFields()) {
					if (id != null) {
						entitymanager.remove(id);
					}
				}

				// TODO: check if thematic is common to other sources
				// remove projects themes
				List<Thematic> thematics = p.getThematics();
				p.getThematics().clear();
				for (Thematic t : thematics) {
					if (t != null) {
						entitymanager.remove(t);
					}
				}
				
				entitymanager.remove(p);

				entitymanager.flush();
				// entitymanager.getTransaction().commit();
			}

			entitymanager.remove(org);

		}		

	}

	private static void removeProject(Project p, EntityManager entitymanager) {
		// TODO Auto-generated method stub

	}

}
