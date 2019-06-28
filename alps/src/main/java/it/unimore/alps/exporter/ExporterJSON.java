package it.unimore.alps.exporter;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import javax.persistence.criteria.CriteriaBuilder;

import com.github.cliftonlabs.json_simple.JsonArray;
import com.github.cliftonlabs.json_simple.JsonObject;

import it.unimore.alps.sql.model.OrganizationActivity;
import it.unimore.alps.sql.model.Badge;
import it.unimore.alps.sql.model.Leader;
import it.unimore.alps.sql.model.Link;
import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationRelation;
import it.unimore.alps.sql.model.Person;
import it.unimore.alps.sql.model.PersonIdentifier;
import it.unimore.alps.sql.model.Project;
import it.unimore.alps.sql.model.Publication;
import it.unimore.alps.sql.model.PublicationIdentifier;
import it.unimore.alps.sql.model.Source;
import it.unimore.alps.sql.model.Thematic;
import it.unimore.alps.sql.model.Theme;

public class ExporterJSON {

	public static void main(String[] args) {
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory("newAlpsDedup");

		EntityManager entitymanager = emfactory.createEntityManager();
		// CriteriaBuilder criteriaBuilder = entitymanager.getCriteriaBuilder();

		// ----------------------------------------------------------------------
		Query query = entitymanager.createQuery("SELECT o FROM Organization o");
		List<Organization> results = query.getResultList();
		System.out.println("Number of organizations: " + results.size());

		List<JsonObject> JSONorgs = exportOrganizations(results);
		
		//writeJSONObjects("/home/matteo/Scrivania/enterprises.json", JSONorgs);
		writeJSONObjects("/home/matteop/enterprises.json", JSONorgs);
		// ----------------------------------------------------------------------
		
		Query queryProject = entitymanager.createQuery("SELECT o FROM Project o");
		List<Project> projects = queryProject.getResultList();
		System.out.println("Number of projects: " + projects.size());

		List<JsonObject> JSONProjects = exportProjects(projects, entitymanager);
		
		//writeJSONObjects("/home/matteo/Scrivania/projects.json", JSONProjects);
		writeJSONObjects("/home/matteop/projects.json", JSONProjects);
		
		
		/*Query queryPublication= entitymanager.createQuery("SELECT p FROM Publication p");
		List<Publication> publications = queryPublication.getResultList();
		System.out.println("Number of publications: " + publications.size());

		List<JsonObject> JSONPublications = exportPublications(publications, entitymanager);
		
		//writeJSONObjects("/home/matteo/Scrivania/publications.json", JSONPublications);
		writeJSONObjects("/home/matteop/publications.json", JSONPublications);*/
		
		/*Query queryPeople = entitymanager.createQuery("SELECT p FROM Person p");
		List<Person> people = queryPeople.getResultList();
		System.out.println("Number of people: " + people.size());

		List<JsonObject> JSONPeople = exportPeople(people, entitymanager);
		
		//writeJSONObjects("/home/matteo/Scrivania/people.json", JSONPeople);
		writeJSONObjects("/home/matteop/people.json", JSONPeople);*/
		// ----------------------------------------------------------------------
		

		entitymanager.close();
		emfactory.close();
	}
	
	public static void writeJSONObjects(String filename, List<JsonObject> JSONObjects) {
		
		try (FileWriter file = new FileWriter(filename)) {
			for(JsonObject JSONobject: JSONObjects) {
				try {
					file.write(JSONobject.toJson() + "\n");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			file.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	public static List<JsonObject> exportOrganizations(List<Organization> organizations) {

		List<String> jsonLines = new ArrayList<String>();

		List<JsonObject> orgs = new ArrayList<>();
		//List<JsonObject> orgsRNSR = new ArrayList<>();
		int numOrgs = 0;
		for (Organization org : organizations) {
			
			if (numOrgs%1000 == 0) {
				System.out.println(numOrgs);
			}
			
			JsonObject o = new JsonObject();
			o.put("id", org.getId());
			//o.put("acronym", org.getAcronym());
			
			JsonArray acronyms = new JsonArray();
			for (String acronym : org.getAcronyms()) {
				acronyms.add(acronym);
			}
			o.put("acronyms", acronyms);
			
			o.put("alias", org.getAlias());
			o.put("label", org.getLabel());
			if (org.getCreationYear() != null) {
				o.put("creationYear", org.getCreationYear().toString());
			} else {
				o.put("creationYear", null);
			}
			o.put("commercialLabel", org.getCommercialLabel());
			
			// address
			JsonObject address = getAddress(org);
			o.put("address", address);

			// finance data
			JsonObject financePrivate = new JsonObject();
			financePrivate.put("revenueRange", org.getFinancePrivateRevenueRange());
			if (org.getFinancePrivateDate() != null) {
				financePrivate.put("date", org.getFinancePrivateDate().toString());
			} else {
				financePrivate.put("date", null);
			}
			financePrivate.put("employes", org.getFinancePrivateEmployees());
			o.put("financePrivate", financePrivate);

			// public organization type
			JsonObject type = new JsonObject();
			type.put("code_categorie", org.getTypeCategoryCode());
			type.put("label", org.getTypeLabel());
			type.put("kind", org.getTypeKind());
			type.put("isPublic", org.getIsPublic());
			o.put("type", type);

			// leaders
			JsonArray leaders = getLeaders(org);
			o.put("leaders", leaders);

			// links
			JsonArray links = getLinks(org);
			o.put("links", links);
			
			// private organization type
			JsonObject organizationType = getOrganizationType(org);
			o.put("companyType", organizationType);
			
			// activities
			JsonArray activities = getActivity(org);
			o.put("activities", activities);

			// relations
			JsonArray relations = getRelations(org);
			o.put("relations", relations);

			//orgs.add(o);

			// sources
			JsonArray sources = new JsonArray();
			for (Source s : org.getSources()) {
				JsonObject source = new JsonObject();
				source.put("label", s.getLabel());
				if (s.getRevisionDate() != null) {
					source.put("revisionDate", s.getRevisionDate().toString());
				}
				source.put("url", s.getUrl());
				sources.add(source);

			}
			o.put("sources", sources);
			
			// badges
			JsonArray badges = getBadges(org);
			o.put("badges", badges);

			/*for (Organization orgC : org.getChildrenOrganizations()) {

				JsonObject organizationChild = new JsonObject();
				organizationChild.put("id", orgC.getId());
				organizationChild.put("acronym", orgC.getAcronym());

				JsonArray alias = new JsonArray();
				alias.add(orgC.getAlias());
				organizationChild.put("alias", alias);
				organizationChild.put("label", orgC.getLabel());

				// organizationChild.put("label", org.getLabel());
				//
				// organizationChild.put("creationYear", org.getCreationYear());
				//
				// organizationChild.put("commercialLabel", org.getCommercialLabel());

				organizationChild.put("address", getAddress(orgC));
				organizationChild.put("companyType", getOrganizationType(orgC));
				organizationChild.put("links", getLinks(orgC));
				organizationChild.put("leaders", getLeaders(orgC));
				organizationChild.put("relations", getRelations(orgC));
				organizationChild.put("activities", getRelations(orgC));
				organizationChild.put("badges", getBadges(orgC));

				JsonArray institutions = new JsonArray();

				JsonObject institution = new JsonObject();
				institution.put("id", org.getId());
				institution.put("label", org.getLabel());
				institution.put("acronym", org.getAcronym());
				institution.put("year", org.getCreationYear());
				institution.put("type", org.getTypeCategoryCode());
				institution.put("url", "structure/" + org.getId());
				institutions.add(institution);

				organizationChild.put("institutions", institutions);

				orgsRNSR.add(organizationChild);
			}*/
			
			// organization hierarchy
			JsonArray children = new JsonArray();
			for (Organization orgC : org.getChildrenOrganizations()) {
				JsonObject child = new JsonObject();
				child.put("id", orgC.getId());
				child.put("label", orgC.getLabel());
				//child.put("acronym", orgC.getAcronym());
				JsonArray childAcronyms = new JsonArray();
				for (String acronym : orgC.getAcronyms()) {
					childAcronyms.add(acronym);
				}
				child.put("acronyms", childAcronyms);
				
				if (orgC.getCreationYear() != null) {
					child.put("year", orgC.getCreationYear().toString());
				} else {
					child.put("year", null);
				}
				child.put("type", orgC.getTypeCategoryCode());
				child.put("url", "structure/" + orgC.getId());
				children.add(child);
				
			}		
			o.put("children", children);
			
			// TODO spinOff from (non ancora gestito)
			
			// TODO: OrganizationExtraFields (parlare con Loic)			
			
			// TODO: organizationIdentifiers (solo se derivano da sorgenti pubbliche/istituzionali)

			orgs.add(o);
			numOrgs++;

		}
		
		return orgs;

	}

	private static JsonArray getRelations(Organization org) {
		JsonArray relations = new JsonArray();

		for (OrganizationRelation rel : org.getOrganizationRelations()) {

			JsonObject relation = new JsonObject();
			relation.put("id", rel.getId());
			relation.put("acronym", rel.getAcronym());
			relation.put("type", rel.getCode());
			relation.put("label", rel.getLabel());
			relation.put("url", rel.getUrl());
			relation.put("logo", rel.getLogo());
			relations.add(relation);
		}
		return relations;
	}

	private static JsonArray getBadges(Organization org) {
		JsonArray badges = new JsonArray();
		for (Badge badge : org.getBadges()) {
			JsonObject b = new JsonObject();
			b.put("code", badge.getCode());
			b.put("label", badge.getLabel());
		}
		return badges;
	}

	private static JsonArray getActivity(Organization org) {
		JsonArray activities = new JsonArray();
		for (OrganizationActivity act : org.getActivities()) {

			JsonObject activity = new JsonObject();
			activity.put("code", act.getCode());
			activity.put("label", act.getLabel());
			activity.put("type", act.getType());
			activities.add(activity);
		}
		return activities;
	}

	private static JsonObject getOrganizationType(Organization org) {
		JsonObject organizationType = new JsonObject();
		if (org.getOrganizationType() != null) {
			organizationType.put("id", org.getOrganizationType().getId());
			organizationType.put("label", org.getOrganizationType().getLabel());
		}
		return organizationType;
	}

	private static JsonObject getAddress(Organization org) {
		JsonObject address = new JsonObject();

		address.put("address", org.getAddress());
		address.put("city", org.getCity());
		address.put("citycode", org.getCityCode());
		address.put("country", org.getCountry());
		address.put("countryCode", org.getCountryCode());

		address.put("postcode", org.getPostcode());
		address.put("urbanUnit", org.getUrbanUnit());
		address.put("urbanUnitCode", org.getUrbanUnitCode());

		JsonObject gps = new JsonObject();
		gps.put("lat", org.getLat());
		gps.put("lon", org.getLon());

		address.put("gps", gps);
		return address;
	}

	private static JsonArray getLeaders(Organization org) {
		JsonArray leaders = new JsonArray();
		for (Person leader : org.getPeople()) {
			JsonObject l = new JsonObject();
			l.put("email", leader.getEmail());
			l.put("firstname", leader.getFirstName());
			l.put("lastname", leader.getLastName());
			l.put("title", leader.getTitle());
			leaders.add(l);
		}
		return leaders;

	}

	private static JsonArray getLinks(Organization org) {
		JsonArray links = new JsonArray();
		for (Link link : org.getLinks()) {
			JsonObject l = new JsonObject();
			l.put("url", link.getUrl());
			l.put("type", link.getType());
			l.put("label", link.getLabel());
			links.add(l);
		}
		return links;
	}

	public static List<JsonObject> exportProjects(List<Project> projects, EntityManager em) {
		List<JsonObject> projectsList = new ArrayList<>();

		int numPrjs = 0;
		for (Project prj : projects) {
			
			if (numPrjs%1000 == 0) {
				System.out.println(numPrjs);
			}

			JsonObject project = new JsonObject();
			project.put("id", prj.getId());
			project.put("acronym", prj.getAcronym());
			project.put("label", prj.getLabel());
			project.put("description", prj.getDescription());
			project.put("duration", prj.getDuration());
			project.put("budget", prj.getBudget());
			project.put("call", prj.getCallId());
			project.put("callLabel", prj.getCallLabel());
			project.put("url", prj.getUrl());
			
			if(prj.getStartDate() != null) {
				project.put("date_debut", prj.getStartDate().toString());
			} else {
				project.put("date_debut", null);
			}
			project.put("type", prj.getType());
			if (prj.getYear() != null) {
				project.put("year", prj.getYear().toString());
			} else {
				project.put("year", null);
			}
			if (prj.getMonth() != null) {
				project.put("month", prj.getMonth().toString());
			} else {
				project.put("month", null);
			}
			
			// thematics
			JsonArray themes = new JsonArray();
			for (Thematic t : prj.getThematics()) {
				// JsonObject theme = new JsonObject();
				themes.add(t.getLabel());
			}
			project.put("themes", themes);

			// organizations
			List<Organization> organizations = getOrgsFromProject(prj, em);
			JsonArray structures = new JsonArray();
			for (Organization org : organizations) {
				JsonObject organization = new JsonObject();
				organization.put("id", org.getId());
				organization.put("label", org.getLabel());
				String link = null;
				if (org.getLinks().size() > 0) {
					link = org.getLinks().get(0).getUrl();
				}
				organization.put("url", link);
				structures.add(organization);
			}
			project.put("structures", structures);

			// sources
			JsonArray sources = new JsonArray();
			for (Source s : prj.getSources()) {
				JsonObject source = new JsonObject();
				source.put("label", s.getLabel());
				if(s.getRevisionDate() != null) {
					source.put("revisionDate", s.getRevisionDate().toString());
				}
				source.put("url", s.getUrl());
				sources.add(source);

			}
			project.put("sources", sources);
			
			// Publications
			List<Publication> pubs = getPubsFromProject(prj, em);
			if (pubs != null) {
				JsonArray pubsJSON = new JsonArray();
				for (Publication pub : pubs) {
					pubsJSON.add(pub.getId());
				}
				project.put("publications", pubsJSON);
			} else {
				project.put("publications", null);
			}
				
			// TODO: ProjectExtraFields (parlare con Loic)
			
			// TODO: ProjectIdentifiers -> solo se derivano da una sorgente pubblica / istituzionale
			
			projectsList.add(project);
			numPrjs++;

		}
		
		return projectsList;
	}
	
	private static List<JsonObject> exportPublications(List<Publication> publications, EntityManager em) {
		List<JsonObject> publicationsList = new ArrayList<>();
		
		int numPubs = 0;
		for (Publication pub : publications) {
			
			if (numPubs%1000 == 0) {
				System.out.println(numPubs);
			}

			JsonObject publication = new JsonObject();
			publication.put("id", pub.getId());
			publication.put("title", pub.getTitle());
			publication.put("summary", pub.getDescription());
			if (pub.getPublicationDate() != null) {
				publication.put("publicationDate", pub.getPublicationDate().toString());
			} else {
				publication.put("publicationDate", null);
			}
			publication.put("type", pub.getType());
			publication.put("link", pub.getUrl());
			
			// sources
			if (pub.getSources() != null) {
				JsonArray sources = new JsonArray();
				for (Source s : pub.getSources()) {
					JsonObject source = new JsonObject();
					source.put("label", s.getLabel());
					if(s.getRevisionDate() != null) {
						source.put("revisionDate", s.getRevisionDate().toString());
					}
					source.put("url", s.getUrl());
					sources.add(source);
				}
				publication.put("source", sources);
			} else {
				publication.put("source", null);
			}
			
			publication.put("subtitle", pub.getSubtitle());
			publication.put("alternativeSummary", null);
			publication.put("linkDocument", null);
			
			// people
			List<Person> authors = pub.getAuthors();
			if (authors != null) {
				JsonArray jsonAuthors = new JsonArray();
				for (Person author: authors) {
					jsonAuthors.add(author.getId());
				}
				publication.put("authors", jsonAuthors);
			} else {
				publication.put("authors", null);
			}
			
			publication.put("thesisDirectors", null);
			
			// TODO: eventualmente aggiungere altri identifiers solo se derivano da sorgenti pubbliche/istituzionali
			List<PublicationIdentifier> pubIds = pub.getPublicationIdentifiers();
			if (pubIds != null) {
				JsonObject identifiers = new JsonObject();
				for(PublicationIdentifier pubId: pubIds) {
					if (pubId.getIdentifierName().equals("thesesfr") || pubId.getIdentifierName().equals("patent")) {
						identifiers.put(pubId.getIdentifierName(), pubId.getIdentifier());
					}
				}
				publication.put("identifiers", identifiers);
			} else {
				publication.put("identifiers", null);
			}
			
			// thematics
			if (pub.getThematics() != null) {
				JsonArray thematics = new JsonArray();
				for (Thematic t : pub.getThematics()) {
					JsonObject thematic = new JsonObject();
					thematic.put("type", t.getClassificationSystem());
					thematic.put("code", t.getCode());
					thematic.put("label", t.getLabel());
					thematics.add(thematic);
				}
				publication.put("thematics", thematics);
			} else {
				publication.put("thematics", null);
			}

			// organizations
			List<Organization> organizations = getOrgsFromPublication(pub, em);
			if (organizations != null) {
				JsonArray structures = new JsonArray();
				for (Organization org : organizations) {
					structures.add(org.getId());
				}
				publication.put("structures", structures);
			} else {
				publication.put("structures", null);
			}
					
			publicationsList.add(publication);
			numPubs++;

		}
		
		return publicationsList;
	}
	
	private static List<JsonObject> exportPeople(List<Person> people, EntityManager em) {
		
		List<JsonObject> peopleList = new ArrayList<>();
		
		int numPeople = 0;
		for (Person p : people) {
			
			if (numPeople%1000 == 0) {
				System.out.println(numPeople);
			}

			JsonObject person = new JsonObject();
			person.put("id", p.getId());
			person.put("firstname", p.getFirstName());
			person.put("lastname", p.getLastName());
			person.put("title", p.getTitle());
			person.put("email", p.getEmail());
			
			// sources
			if (p.getSources() != null) {
				JsonArray sources = new JsonArray();
				for (Source s : p.getSources()) {
					JsonObject source = new JsonObject();
					source.put("label", s.getLabel());
					if(s.getRevisionDate() != null) {
						source.put("revisionDate", s.getRevisionDate().toString());
					}
					source.put("url", s.getUrl());
					sources.add(source);
				}
				person.put("sources", sources);
			} else {
				person.put("sources", null);
			}
			
			// TODO: solo se derivano da sorgenti pubbliche/istituzionali
			List<PersonIdentifier> personIds = p.getPersonIdentifiers();
			if (personIds != null) {
				JsonArray identifiers = new JsonArray();
				
				for(PersonIdentifier personId: personIds) {
					JsonObject identifier = new JsonObject();
					identifier.put("id", personId.getIdentifier());
					identifier.put("provenance", personId.getIdentifierName());
					identifier.put("link", personId.getProvenance());
					identifiers.add(identifier);
				}
				person.put("identifiers", identifiers);
			} else {
				person.put("identifiers", null);
			}
			
			// organizations
			List<Organization> organizations = getOrgsFromPerson(p, em);
			if (organizations != null) {
				JsonArray structures = new JsonArray();
				for (Organization org : organizations) {
					structures.add(org.getId());
				}
				person.put("structures", structures);
			} else {
				person.put("structures", null);
			}
				
			peopleList.add(person);
			numPeople++;

		}
		
		return peopleList;
	}

	private static List<Organization> getOrgsFromProject(Project prj, EntityManager entitymanager) {
		Query query = entitymanager.createQuery("SELECT o FROM Organization o WHERE :prj  MEMBER OF o.projects");
		query.setParameter("prj", prj);

		List<Organization> results = query.getResultList();

		return results;
	}
	
	private static List<Organization> getOrgsFromPublication(Publication pub, EntityManager entitymanager) {
		Query query = entitymanager.createQuery("SELECT o FROM Organization o WHERE :pub  MEMBER OF o.publications");
		query.setParameter("pub", pub);

		List<Organization> results = query.getResultList();

		return results;
	}
	
	
	private static List<Organization> getOrgsFromPerson(Person p, EntityManager entitymanager) {
		Query query = entitymanager.createQuery("SELECT o FROM Organization o WHERE :p MEMBER OF o.people");
		query.setParameter("p", p);

		List<Organization> results = query.getResultList();

		return results;
	}
	
	private static List<Publication> getPubsFromProject(Project prj, EntityManager entitymanager) {
		Query query = entitymanager.createQuery("SELECT p FROM Publication p WHERE :prj MEMBER OF p.projects");
		query.setParameter("prj", prj);

		List<Publication> results = query.getResultList();

		return results;
	}
	
}
