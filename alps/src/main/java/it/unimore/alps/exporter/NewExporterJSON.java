package it.unimore.alps.exporter;

import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import javax.swing.border.EtchedBorder;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.github.cliftonlabs.json_simple.JsonArray;
import com.github.cliftonlabs.json_simple.JsonObject;

import it.unimore.alps.sql.model.OrganizationActivity;
import it.unimore.alps.sql.model.OrganizationExtraField;
import it.unimore.alps.sql.model.OrganizationIdentifier;
import it.unimore.alps.sql.model.AlternativeName;
import it.unimore.alps.sql.model.Badge;
import it.unimore.alps.sql.model.ExternalParticipant;
import it.unimore.alps.sql.model.Leader;
import it.unimore.alps.sql.model.Link;
import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationRelation;
import it.unimore.alps.sql.model.Person;
import it.unimore.alps.sql.model.PersonIdentifier;
import it.unimore.alps.sql.model.Project;
import it.unimore.alps.sql.model.ProjectExtraField;
import it.unimore.alps.sql.model.ProjectIdentifier;
import it.unimore.alps.sql.model.Publication;
import it.unimore.alps.sql.model.PublicationIdentifier;
import it.unimore.alps.sql.model.Source;
import it.unimore.alps.sql.model.Thematic;
import it.unimore.alps.sql.model.Theme;

public class NewExporterJSON {

	public static void main(String[] args) {

		// read parameter for the db

		CommandLine commandLine;

		Option DB = Option.builder("DB").hasArg().required(true).desc("DB. ").longOpt("DB").build();
		Option completeExportOption = Option.builder("complete").hasArg().desc("Export all entities (also the ones disconnected). The default mode exports only connected entities.").longOpt("complete").build();

		Options options = new Options();

		options.addOption(DB);
		options.addOption(completeExportOption);

		CommandLineParser parser = new DefaultParser();

		String db = null;
		boolean completeExport = false;
		try {
			commandLine = parser.parse(options, args);
			
			System.out.println("----------------------------");
			System.out.println("OPTIONS:");

			if (commandLine.hasOption("DB")) {
				db = commandLine.getOptionValue("DB");
				System.out.println("Database: " + db);
			} else {
				System.out.println("Source database is not provided. Use the DB option.");
				System.exit(1);
			}
			
			if (commandLine.hasOption("DB")) {
				completeExport = true;
				System.out.println("Complete export");
			} else {
				System.out.println("Partial export");
			}			
			
			System.out.println("----------------------------\n");

		} catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);

		EntityManager entitymanager = emfactory.createEntityManager();

		// ----------------------------------------------------------------------
		// retrieve entities
		List<Organization> orgs = retrieveOrganizations(entitymanager);
		List<Project> prjs = retrieveProjects(entitymanager);
		List<Publication> pubs = retrievePublications(entitymanager);
		List<Person> people = retrievePeople(entitymanager);

		entitymanager.close();
		emfactory.close();
		// ----------------------------------------------------------------------

		// ----------------------------------------------------------------------
		// export organizations
		List<JsonObject> JSONorgs = exportOrganizations(orgs);
		writeJSONObjects("enterprises.json", JSONorgs);
		// ----------------------------------------------------------------------

		// ----------------------------------------------------------------------
		// export projects
		List<JsonObject> JSONProjects = exportProjects(prjs, orgs, pubs, completeExport);
		String projectFileName = null;
		if (completeExport) {
			projectFileName = "projects_complete.json";
		} else {
			projectFileName = "projects.json";
		}
		writeJSONObjects(projectFileName, JSONProjects);
		// ----------------------------------------------------------------------

		// ----------------------------------------------------------------------
		// export publications
		List<JsonObject> JSONPublications = exportPublications(pubs, orgs, completeExport, db);
		writeJSONObjects("publications.json", JSONPublications);
		// ----------------------------------------------------------------------

		// ----------------------------------------------------------------------
		// export people
//		List<JsonObject> JSONPeople = exportPeople(people, orgs, completeExport);
//		writeJSONObjects("people.json", JSONPeople);
		// ----------------------------------------------------------------------

	}

	public static List<Organization> retrieveOrganizations(EntityManager entitymanager) {

		entitymanager.getTransaction().begin();
		Query query_org = entitymanager.createQuery("Select o FROM Organization o");
		List<Organization> orgs = query_org.getResultList();

		entitymanager.getTransaction().commit();
		System.out.println("Retrieved " + orgs.size() + " organizations");

		return orgs;
	}

	public static List<Project> retrieveProjects(EntityManager entitymanager) {

		entitymanager.getTransaction().begin();
		Query query_prj = entitymanager.createQuery("Select o FROM Project o");
		List<Project> prjs = query_prj.getResultList();

		entitymanager.getTransaction().commit();
		System.out.println("Retrieved " + prjs.size() + " projects");

		return prjs;
	}

	public static List<Publication> retrievePublications(EntityManager entitymanager) {

		entitymanager.getTransaction().begin();
		Query query_prj = entitymanager.createQuery("Select o FROM Publication o");
		List<Publication> pubs = query_prj.getResultList();

		entitymanager.getTransaction().commit();
		System.out.println("Retrieved " + pubs.size() + " publications");

		return pubs;
	}

	public static List<Person> retrievePeople(EntityManager entitymanager) {

		entitymanager.getTransaction().begin();
		Query query_prj = entitymanager.createQuery("Select o FROM Person o");
		List<Person> people = query_prj.getResultList();

		entitymanager.getTransaction().commit();
		System.out.println("Retrieved " + people.size() + " people");

		return people;
	}

	public static void writeJSONObjects(String filename, List<JsonObject> JSONObjects) {

		//try (FileWriter file = new FileWriter(filename)) {
		try (Writer file = new OutputStreamWriter(new FileOutputStream(filename), StandardCharsets.UTF_8);) {
			for (JsonObject JSONobject : JSONObjects) {
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
		// List<JsonObject> orgsRNSR = new ArrayList<>();
		int numOrgs = 0;
		for (Organization org : organizations) {

			if (numOrgs % 1000 == 0) {
				System.out.println(numOrgs);
			}

			if (org != null) {
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
				String isPublic = org.getIsPublic();
				if (isPublic.equals("undefined")) {
					type.put("isPublic", "Not available");
				} else {
					type.put("isPublic", org.getIsPublic());
				}
				o.put("type", type);
	
				// leaders
				JsonArray leaders = getLeaders(org);
				o.put("leaders", leaders);
				
				// staff
				JsonArray people = getPeople(org);
				o.put("staff", people);
	
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
	
				// orgs.add(o);
	
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
	
				/*
				 * for (Organization orgC : org.getChildrenOrganizations()) {
				 * 
				 * JsonObject organizationChild = new JsonObject(); organizationChild.put("id",
				 * orgC.getId()); organizationChild.put("acronym", orgC.getAcronym());
				 * 
				 * JsonArray alias = new JsonArray(); alias.add(orgC.getAlias());
				 * organizationChild.put("alias", alias); organizationChild.put("label",
				 * orgC.getLabel());
				 * 
				 * // organizationChild.put("label", org.getLabel()); // //
				 * organizationChild.put("creationYear", org.getCreationYear()); // //
				 * organizationChild.put("commercialLabel", org.getCommercialLabel());
				 * 
				 * organizationChild.put("address", getAddress(orgC));
				 * organizationChild.put("companyType", getOrganizationType(orgC));
				 * organizationChild.put("links", getLinks(orgC));
				 * organizationChild.put("leaders", getLeaders(orgC));
				 * organizationChild.put("relations", getRelations(orgC));
				 * organizationChild.put("activities", getRelations(orgC));
				 * organizationChild.put("badges", getBadges(orgC));
				 * 
				 * JsonArray institutions = new JsonArray();
				 * 
				 * JsonObject institution = new JsonObject(); institution.put("id",
				 * org.getId()); institution.put("label", org.getLabel());
				 * institution.put("acronym", org.getAcronym()); institution.put("year",
				 * org.getCreationYear()); institution.put("type", org.getTypeCategoryCode());
				 * institution.put("url", "structure/" + org.getId());
				 * institutions.add(institution);
				 * 
				 * organizationChild.put("institutions", institutions);
				 * 
				 * orgsRNSR.add(organizationChild); }
				 */
	
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
	
				// organization identifiers
				JsonArray orgIdentifiers = getOrgIdentifiers(org);
				o.put("identifiers", orgIdentifiers);
	
				JsonArray orgExtraFields = getOrgExtraFields(org);
				o.put("extra_fields", orgExtraFields);
				
				// alternative names
				JsonArray alternativeNames = new JsonArray();
				for (AlternativeName an: org.getAlternativeNames()) {
					JsonObject altName = new JsonObject();
					altName.put("label", an.getLabel());
					
					JsonArray altNameSources = new JsonArray();
					for (Source s : an.getSources()) {
						JsonObject source = new JsonObject();
						source.put("label", s.getLabel());
						if (s.getRevisionDate() != null) {
							source.put("revisionDate", s.getRevisionDate().toString());
						}
						source.put("url", s.getUrl());
						altNameSources.add(source);
					}
					altName.put("sources", altNameSources);
					
					alternativeNames.add(altName);
	
				}
				o.put("alternative_names", alternativeNames);
	
				orgs.add(o);
				numOrgs++;
			}

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

	private static JsonArray getOrgIdentifiers(Organization org) {
		JsonArray identifiers = new JsonArray();
		for (OrganizationIdentifier orgId : org.getOrganizationIdentifiers()) {

			JsonObject id = new JsonObject();
			id.put("type", orgId.getIdentifierName());
			id.put("id", orgId.getIdentifier());
			id.put("provenance", orgId.getProvenance());
			id.put("link", orgId.getLink());
			id.put("visible", orgId.isVisibility());
			identifiers.add(id);
		}
		return identifiers;
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
		//address.put("urbanUnit", org.getUrbanUnit());
		//address.put("urbanUnitCode", org.getUrbanUnitCode());

		// geo-coordinates
		List<OrganizationExtraField> orgExtraFields = org.getOrganizationExtraFields();		
		if (orgExtraFields != null) {
			
			int numMergedOrgs = 0;
			List<OrganizationIdentifier> orgIds = org.getOrganizationIdentifiers();
			if (orgIds != null) {
				for(OrganizationIdentifier orgId: orgIds) {
					if (orgId.getIdentifierName().equals("lid")) {
						numMergedOrgs += 1;
					}
				}
			}						
			
			//boolean fakeGPSFound = false;
			int numFakeGPS = 0;
			for (OrganizationExtraField oef: orgExtraFields) {
				if (oef.getFieldKey().equals("fake gps")) {
					//fakeGPSFound = true;
					//break;
					numFakeGPS += 1;
				}
			}
			
			if (numFakeGPS == numMergedOrgs) {
				JsonObject gps = new JsonObject();
				gps.put("lat", null);
				gps.put("lon", null);				
				address.put("gps", gps);
				
			} else {
				JsonObject gps = new JsonObject();
				gps.put("lat", org.getLat());
				gps.put("lon", org.getLon());				
				address.put("gps", gps);
			}
			
		} else {
			JsonObject gps = new JsonObject();
			gps.put("lat", org.getLat());
			gps.put("lon", org.getLon());				
			address.put("gps", gps);
		}		
		
		// nuts codes
		JsonObject nuts = new JsonObject();
		nuts.put("nuts1", org.getNutsLevel1());
		nuts.put("nuts2", org.getNutsLevel2());				
		nuts.put("nuts3", org.getNutsLevel3());
		address.put("nuts", nuts);
		
		JsonArray sources = new JsonArray();
		for (Source s : org.getAddressSources()) {
			JsonObject source = new JsonObject();
			source.put("label", s.getLabel());
			if (s.getRevisionDate() != null) {
				source.put("revisionDate", s.getRevisionDate().toString());
			}
			source.put("url", s.getUrl());
			sources.add(source);
		}
		address.put("sources", sources);
		
		return address;
	}

	private static JsonArray getLeaders(Organization org) {
		JsonArray leaders = new JsonArray();
		for (Leader leader : org.getLeaders()) {
			JsonObject l = new JsonObject();
			l.put("email", leader.getEmail());
			l.put("firstname", leader.getFirstName());
			l.put("lastname", leader.getLastName());
			l.put("title", leader.getTitle());
			
			JsonArray sources = new JsonArray();
			for (Source s : leader.getSources()) {
				JsonObject source = new JsonObject();
				source.put("label", s.getLabel());
				if (s.getRevisionDate() != null) {
					source.put("revisionDate", s.getRevisionDate().toString());
				}
				source.put("url", s.getUrl());
				sources.add(source);
			}
			l.put("sources", sources);
			
			leaders.add(l);			
		}
		return leaders;

	}
	
	private static JsonArray getPeople(Organization org) {
		JsonArray leaders = new JsonArray();
		for (Person leader : org.getPeople()) {
			JsonObject l = new JsonObject();
			l.put("email", leader.getEmail());
			l.put("firstname", leader.getFirstName());
			l.put("lastname", leader.getLastName());
			l.put("title", leader.getTitle());
			
			JsonArray sources = new JsonArray();
			for (Source s : leader.getSources()) {
				JsonObject source = new JsonObject();
				source.put("label", s.getLabel());
				if (s.getRevisionDate() != null) {
					source.put("revisionDate", s.getRevisionDate().toString());
				}
				source.put("url", s.getUrl());
				sources.add(source);
			}
			l.put("sources", sources);
			
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
			
			JsonArray sources = new JsonArray();
			for (Source s : link.getSources()) {
				JsonObject source = new JsonObject();
				source.put("label", s.getLabel());
				if (s.getRevisionDate() != null) {
					source.put("revisionDate", s.getRevisionDate().toString());
				}
				source.put("url", s.getUrl());
				sources.add(source);
			}
			l.put("sources", sources);
			
			links.add(l);
		}
		return links;
	}

	public static List<JsonObject> exportProjects(List<Project> projects, List<Organization> sourceOrgs,
			List<Publication> sourcePubs, boolean completeExport) {
		List<JsonObject> projectsList = new ArrayList<>();

		Map<Integer, List<Organization>> orgPrjMap = new HashMap<>();
		Map<Integer, List<Publication>> pubPrjMap = new HashMap<>();
		if (projects.size() > 0) {
			// create org-projects map
			for (Organization org : sourceOrgs) {
				List<Project> orgsProjects = org.getProjects();
				if (orgsProjects != null) {
					for (Project prj : orgsProjects) {
						int key = prj.getId();
						if (orgPrjMap.get(key) != null) {
							orgPrjMap.get(key).add(org);
						} else {
							List<Organization> orgs = new ArrayList<>();
							orgs.add(org);
							orgPrjMap.put(key, orgs);
						}
					}
				}
			}

			// create publication-projects map
			for (Publication pub : sourcePubs) {
				List<Project> pubsProjects = pub.getProjects();
				if (pubsProjects != null) {
					for (Project prj : pubsProjects) {
						int key = prj.getId();
						if (pubPrjMap.get(key) != null) {
							pubPrjMap.get(key).add(pub);
						} else {
							List<Publication> pubs = new ArrayList<>();
							pubs.add(pub);
							pubPrjMap.put(key, pubs);
						}
					}
				}
			}
		}

		int numPrjs = 0;
		for (Project prj : projects) {

			if (numPrjs % 1000 == 0) {
				System.out.println(numPrjs);
			}
			
			if (!completeExport && orgPrjMap.get(prj.getId()) == null && pubPrjMap.get(prj.getId()) == null) {
				continue;
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

			if (prj.getStartDate() != null) {
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
			// List<Organization> organizations = getOrgsFromProject(sourceOrgs, prj);
			List<Organization> organizations = orgPrjMap.get(prj.getId());
			JsonArray structures = new JsonArray();
			if (organizations != null) {
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
				
				// external participants
				for (ExternalParticipant ep : prj.getExternalParticipants()) {
					JsonObject extP = new JsonObject();
					extP.put("id", null);
					extP.put("label", ep.getLabel());
					extP.put("url", ep.getUrl());
					structures.add(extP);
				}
				
				project.put("structures", structures);
			} else {
				project.put("structures", null);
			}

			// sources
			JsonArray sources = new JsonArray();
			for (Source s : prj.getSources()) {
				JsonObject source = new JsonObject();
				source.put("label", s.getLabel());
				if (s.getRevisionDate() != null) {
					source.put("revisionDate", s.getRevisionDate().toString());
				}
				source.put("url", s.getUrl());
				sources.add(source);

			}
			project.put("sources", sources);

			// Publications
			// List<Publication> pubs = getPubsFromProject(sourcePubs, prj);
			List<Publication> pubs = pubPrjMap.get(prj.getId());
			JsonArray pubsJSON = new JsonArray();
			if (pubs != null) {
				for (Publication pub : pubs) {
					pubsJSON.add(pub.getId());
				}
				project.put("publications", pubsJSON);
			} else {
				project.put("publications", null);
			}

			JsonArray prjExtraFields = getPrjExtraFields(prj);
			project.put("extra_fields", prjExtraFields);

			// organization identifiers
			JsonArray prjIdentifiers = getPrjIdentifiers(prj);
			project.put("identifiers", prjIdentifiers);					

			projectsList.add(project);
			numPrjs++;

		}

		return projectsList;
	}

	private static JsonArray getPrjIdentifiers(Project prj) {
		JsonArray identifiers = new JsonArray();
		for (ProjectIdentifier prjId : prj.getProjectIdentifiers()) {

			JsonObject id = new JsonObject();
			id.put("type", prjId.getIdentifierName());
			id.put("id", prjId.getIdentifier());
			id.put("provenance", prjId.getProvenance());
			id.put("link", null);
			if (prjId.isVisibility()) {
				id.put("visible", "true");
			} else {
				id.put("visible", "false");
			}
			/*if (prjId.isVisibility()) {
				identifiers.add(id);
			}*/
			identifiers.add(id);
		}
		return identifiers;
	}

	private static JsonArray getOrgExtraFields(Organization org) {

		JsonArray extraIdentifiers = new JsonArray();

		//org.getOrganizationIdentifiers();

		for (OrganizationExtraField extra : org.getOrganizationExtraFields()) {			
			
			if (extra.getFieldKey().startsWith("crawled")) {
				
				//JsonObject crawledExtraFieldObject = new JsonObject();
				//JsonArray cawledExtraFieldValueList = new JsonArray();							
				String[] extraFieldValueList = extra.getFieldValue().split(",");
				int counter = 0;
				for(String extraFieldValue: extraFieldValueList) {	
					counter += 1;
					JsonObject extraField = new JsonObject();
					extraField.put("type", extra.getFieldKey() + " " + counter);
					extraField.put("id", extraFieldValue);
					extraField.put("provenance", "");
					extraField.put("link", "");
					extraField.put("visible", extra.isVisibility());
					//cawledExtraFieldValueList.add(extraField);
					extraIdentifiers.add(extraField);
				}
				
				//crawledExtraFieldObject.put(extra.getFieldKey(), cawledExtraFieldValueList);
				//extraIdentifiers.add(crawledExtraFieldObject);
				
				
			} else {

				JsonObject id = new JsonObject();
				id.put("id", extra.getFieldValue());
				id.put("type", extra.getFieldKey());
				id.put("provenance", "");
				id.put("link", "");
				id.put("visible", extra.isVisibility());
				/*if (extra.isVisibility()) {
					id.put("visible", "true");
				} else {
					id.put("visible", "false");
				}*/
				/*if (extra.isVisibility()) {
					extraIdentifiers.add(id);
				}*/
				extraIdentifiers.add(id);
			}
		}

		return extraIdentifiers;
	}

	private static JsonArray getPrjExtraFields(Project prj) {

		JsonArray extraIdentifiers = new JsonArray();

		for (ProjectExtraField extra : prj.getProjectExtraFields()) {
			JsonObject id = new JsonObject();

			id.put("id", extra.getFieldValue());
			id.put("type", extra.getFieldKey());
			id.put("provenance", "");
			id.put("link", "");
			if (extra.isVisibility()) {
				id.put("visible", "true");
			} else {
				id.put("visible", "false");
			}
			/*if (extra.isVisibility()) {
				extraIdentifiers.add(id);
			}*/
			extraIdentifiers.add(id);
		}

		return extraIdentifiers;
	}

	private static List<JsonObject> exportPublications(List<Publication> publications, List<Organization> sourceOrgs, boolean completeExport, String db) {

		List<JsonObject> publicationsList = new ArrayList<>();

		// create a map where to connect a publication with its organizations
		Map<Integer, List<Organization>> orgPubMap = new HashMap<>();
		if (publications.size() > 0) {
			for (Organization org : sourceOrgs) {
				List<Publication> orgsPubs = org.getPublications();
				if (orgsPubs != null) {
					for (Publication pub : orgsPubs) {
						int key = pub.getId();
						if (orgPubMap.get(key) != null) {
							orgPubMap.get(key).add(org);
						} else {
							List<Organization> orgs = new ArrayList<>();
							orgs.add(org);
							orgPubMap.put(key, orgs);
						}
					}
				}
			}
		}

		// create a map where to connect a publication with its identifiers
		Map<Integer, List<PublicationIdentifier>> pubPubIdMap = new HashMap<>();
		for (Publication pub : publications) {
			List<PublicationIdentifier> pubIds = new ArrayList<PublicationIdentifier>(); 
			for(PublicationIdentifier pId: pub.getPublicationIdentifiers()) {
				pId.getIdentifier();
				pubIds.add(pId);
			}
			if (pubIds != null) {
				pubPubIdMap.put(pub.getId(), pubIds);
			}
		}

		int numPubs = 0;

		// publications = publications.subList(0, 200000);
		// create a json object from each publication
		// this data format conversion process can be execute in parallel
		Stream<JsonObject> a = publications.parallelStream().map(new Function<Publication, JsonObject>() {

			@Override
			public JsonObject apply(Publication pub) {
				
				//EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
				//EntityManager entitymanager = emfactory.createEntityManager();
				
				
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
						if (s.getRevisionDate() != null) {
							source.put("revisionDate", s.getRevisionDate().toString());
						}
						source.put("url", s.getUrl());
						sources.add(source);
					}
					publication.put("sources", sources);
					//publication.put("source", null);
				} else {
					publication.put("sources", null);
				}

				publication.put("subtitle", pub.getSubtitle());
				publication.put("alternativeSummary", null);
				publication.put("linkDocument", null);

				// people
				List<Person> authors = pub.getAuthors();
				if (authors != null) {
					JsonArray jsonAuthors = new JsonArray();
					int progAuthorNumber = 1;
					for (Person author : authors) {
						if (author != null) {
							JsonObject jsonAuthor = new JsonObject();
							jsonAuthor.put("num_auteur", progAuthorNumber);
							jsonAuthor.put("auteur_idref", null);
							if (author.getFirstName() != null && author.getLastName() != null) {
								jsonAuthor.put("auteur_nom_prenom", author.getFirstName() + author.getLastName());
							} else {
								jsonAuthor.put("auteur_nom_prenom", null);
							}
							jsonAuthor.put("firstName", author.getFirstName());
							jsonAuthor.put("lastName", author.getLastName());
							jsonAuthor.put("idref", null);
							jsonAuthor.put("affiliations", null);
							jsonAuthors.add(jsonAuthor);

							progAuthorNumber++;
						}
					}
					publication.put("authors", jsonAuthors);
				} else {
					publication.put("authors", null);
				}

				publication.put("thesisDirectors", null);

				// TODO: eventualmente aggiungere altri identifiers solo se derivano da sorgenti
				// pubbliche/istituzionali

				/*if (pub.getSources() != null) {
					JsonArray sources = new JsonArray();
					for (Source s : pub.getSources()) {
						JsonObject source = new JsonObject();
						source.put("label", s.getLabel());
						if (s.getRevisionDate() != null) {
							source.put("revisionDate", s.getRevisionDate().toString());
						}
						source.put("url", s.getUrl());
						sources.add(source);
					}
					publication.put("sources", sources);
				} else {
					publication.put("sources", null);
				}*/

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
				// List<Organization> organizations = getOrgsFromPublication(sourceOrgs, pub);
				List<Organization> organizations = orgPubMap.get(pub.getId());
				JsonArray structures = new JsonArray();
				if (organizations != null) {
					// System.out.println("Found structures");
					for (Organization org : organizations) {
						structures.add(org.getId());
					}
					publication.put("structures", structures);
				} else {
					// System.out.println("Orgs null");
					publication.put("structures", null);
				}

				// projects
				List<Project> projects = pub.getProjects();
				JsonArray projectsRelation = new JsonArray();
				if (organizations != null) {
					// System.out.println("Found structures");
					for (Project prj : projects) {
						projectsRelation.add(prj.getId());
					}
					publication.put("projects", structures);
				} else {
					// System.out.println("Orgs null");
					publication.put("projects", null);
				}

				/*
				 * if (organizations != null) { System.out.println("Orgs: "); for (Organization
				 * org: organizations) { System.out.println(org.getLabel()); } }
				 */

				if (pubPubIdMap.containsKey(pub.getId())) {
					JsonArray ids = new JsonArray();
					for (PublicationIdentifier pubId : pubPubIdMap.get(pub.getId())) {
						JsonObject identifier = new JsonObject();

						identifier.put("type", pubId.getIdentifierName());
						identifier.put("id", pubId.getIdentifier());
						identifier.put("provenance", pubId.getProvenance());
						identifier.put("link", null);
						ids.add(identifier);
					}
					
					publication.put("identifiers", ids);
				} else {
					publication.put("identifiers", null);
				}
				
				//entitymanager.close();
				//emfactory.close();

				return publication;
			}
		});

		List<JsonObject> results = a.collect(Collectors.toList());
		return results;

		// for (Publication pub : publications) {
		//
		// if (numPubs%1000 == 0) {
		// System.out.println(numPubs);
		// }
		//
		// JsonObject publication = new JsonObject();
		// publication.put("id", pub.getId());
		// publication.put("title", pub.getTitle());
		// publication.put("summary", pub.getDescription());
		// if (pub.getPublicationDate() != null) {
		// publication.put("publicationDate", pub.getPublicationDate().toString());
		// } else {
		// publication.put("publicationDate", null);
		// }
		// publication.put("type", pub.getType());
		// publication.put("link", pub.getUrl());
		//
		// // sources
		// if (pub.getSources() != null) {
		// JsonArray sources = new JsonArray();
		// for (Source s : pub.getSources()) {
		// JsonObject source = new JsonObject();
		// source.put("label", s.getLabel());
		// if(s.getRevisionDate() != null) {
		// source.put("revisionDate", s.getRevisionDate().toString());
		// }
		// source.put("url", s.getUrl());
		// sources.add(source);
		// }
		// publication.put("source", sources);
		// } else {
		// publication.put("source", null);
		// }
		//
		// publication.put("subtitle", pub.getSubtitle());
		// publication.put("alternativeSummary", null);
		// publication.put("linkDocument", null);
		//
		// // people
		// List<Person> authors = pub.getAuthors();
		// if (authors != null) {
		// JsonArray jsonAuthors = new JsonArray();
		// for (Person author: authors) {
		// jsonAuthors.add(author.getId());
		// }
		// publication.put("authors", jsonAuthors);
		// } else {
		// publication.put("authors", null);
		// }
		//
		// publication.put("thesisDirectors", null);
		//
		// // TODO: eventualmente aggiungere altri identifiers solo se derivano da
		// sorgenti pubbliche/istituzionali
		// List<PublicationIdentifier> pubIds = pub.getPublicationIdentifiers();
		// if (pubIds != null) {
		// JsonObject identifiers = new JsonObject();
		// for(PublicationIdentifier pubId: pubIds) {
		// if (pubId.getIdentifierName().equals("thesesfr") ||
		// pubId.getIdentifierName().equals("patent")) {
		// identifiers.put(pubId.getIdentifierName(), pubId.getIdentifier());
		// }
		// }
		// publication.put("identifiers", identifiers);
		// } else {
		// publication.put("identifiers", null);
		// }
		//
		// // thematics
		// if (pub.getThematics() != null) {
		// JsonArray thematics = new JsonArray();
		// for (Thematic t : pub.getThematics()) {
		// JsonObject thematic = new JsonObject();
		// thematic.put("type", t.getClassificationSystem());
		// thematic.put("code", t.getCode());
		// thematic.put("label", t.getLabel());
		// thematics.add(thematic);
		// }
		// publication.put("thematics", thematics);
		// } else {
		// publication.put("thematics", null);
		// }
		//
		// // organizations
		// //List<Organization> organizations = getOrgsFromPublication(sourceOrgs, pub);
		// List<Organization> organizations = orgPubMap.get(pub.getId());
		// JsonArray structures = new JsonArray();
		// if (organizations != null) {
		// for (Organization org : organizations) {
		// structures.add(org.getId());
		// }
		// publication.put("structures", structures);
		// } else {
		// publication.put("structures", null);
		// }
		//
		// publicationsList.add(publication);
		// numPubs++;
		//
		// }

		// return publicationsList;
	}

	private static List<JsonObject> exportPeople(List<Person> people, List<Organization> sourceOrgs, boolean completeExport) {

		List<JsonObject> peopleList = new ArrayList<>();

		Map<Integer, List<Organization>> orgPersonMap = new HashMap<>();
		if (people.size() > 0) {
			// create org-people map
			for (Organization org : sourceOrgs) {
				List<Person> orgsPeople = org.getPeople();
				if (orgsPeople != null) {
					for (Person per : orgsPeople) {
						int key = per.getId();
						if (orgPersonMap.get(key) != null) {
							orgPersonMap.get(key).add(org);
						} else {
							List<Organization> orgs = new ArrayList<>();
							orgs.add(org);
							orgPersonMap.put(key, orgs);
						}
					}
				}
			}
		}

		int numPeople = 0;
		for (Person p : people) {

			if (numPeople % 1000 == 0) {
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
					if (s.getRevisionDate() != null) {
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

				for (PersonIdentifier personId : personIds) {
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
			// List<Organization> organizations = getOrgsFromPerson(sourceOrgs, p);
			List<Organization> organizations = orgPersonMap.get(p.getId());
			JsonArray structures = new JsonArray();
			if (organizations != null) {
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

	private static List<Organization> getOrgsFromProject(List<Organization> allOrgs, Project prj) {

		List<Organization> results = new ArrayList<>();

		for (Organization org : allOrgs) {
			List<Project> orgsProjects = org.getProjects();
			if (orgsProjects != null) {
				if (orgsProjects.contains(prj)) {
					results.add(org);
				}
			}
		}

		return results;
	}

	private static List<Organization> getOrgsFromPublication(List<Organization> allOrgs, Publication pub) {

		List<Organization> results = new ArrayList<>();

		for (Organization org : allOrgs) {
			List<Publication> orgsPublications = org.getPublications();
			if (orgsPublications != null) {
				if (orgsPublications.contains(pub)) {
					results.add(org);
				}
			}
		}

		return results;
	}

	private static List<Organization> getOrgsFromPerson(List<Organization> allOrgs, Person p) {

		List<Organization> results = new ArrayList<>();

		for (Organization org : allOrgs) {
			List<Person> orgsPeople = org.getPeople();
			if (orgsPeople != null) {
				if (orgsPeople.contains(org)) {
					results.add(org);
				}
			}
		}

		return results;
	}

	private static List<Publication> getPubsFromProject(List<Publication> allPubs, Project prj) {

		List<Publication> results = new ArrayList<>();

		for (Publication pub : allPubs) {
			List<Project> pubsProjects = pub.getProjects();
			if (pubsProjects != null) {
				if (pubsProjects.contains(prj)) {
					results.add(pub);
				}
			}
		}

		return results;
	}

}
