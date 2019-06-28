package it.unimore.alps.scanr.importer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import it.unimore.alps.sources.cnr.CnrImporter;
import it.unimore.alps.sql.model.Badge;
import it.unimore.alps.sql.model.ExternalParticipant;
import it.unimore.alps.sql.model.Leader;
import it.unimore.alps.sql.model.Link;
import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationActivity;
import it.unimore.alps.sql.model.OrganizationExtraField;
import it.unimore.alps.sql.model.OrganizationIdentifier;
import it.unimore.alps.sql.model.OrganizationRelation;
import it.unimore.alps.sql.model.OrganizationType;
import it.unimore.alps.sql.model.Person;
import it.unimore.alps.sql.model.Project;
import it.unimore.alps.sql.model.ProjectExtraField;
import it.unimore.alps.sql.model.ProjectIdentifier;
import it.unimore.alps.sql.model.Publication;
import it.unimore.alps.sql.model.PublicationIdentifier;
import it.unimore.alps.sql.model.Source;
import it.unimore.alps.sql.model.SpinoffFrom;
import it.unimore.alps.sql.model.Thematic;
import it.unimore.alps.sql.model.Theme;

public class ScanRImporter {

	private Reader reader;															// main organization file reader
	private Reader readerRNSR;														// secondary organization file reader
	private Reader projectReader;													// project file reader
	private Reader publicationReader;												// publication file reader
	private String sourceName = "ScanR";											// data source name
	private String sourceUrl = "https://scanr.enseignementsup-recherche.gouv.fr/";	// data source url
	private String sourceRevisionDate = "01-06-2019";								// data source revision date

	public static void main(String[] args) {

		// BEGIN: INPUT PARAMETERS ------------------------------------------------------
		CommandLine commandLine;
		// main organization file reader
		Option orgsFileOption = Option.builder("orgsFile").hasArg().required(true)
				.desc("The file that contains ScanR organizations data. ").longOpt("organizationsFile").build();
		// secondary organization file reader
		Option orgsRNSRFileOption = Option.builder("orgsRNSRFile").hasArg().required(true)
				.desc("The file that contains RNSR organizations data. ").longOpt("organizationsRNSRFile").build();
		// project file reader
		Option projectsFileOption = Option.builder("projectsFile").hasArg().required(true)
				.desc("The file that contains ScanR projects data. ").longOpt("projectsFile").build();
		// publication file reader
		Option publicationsFileOption = Option.builder("publicationsFile").hasArg().required(true)
				.desc("The file that contains ScanR publications data. ").longOpt("publicationsFile").build();
		// database where to import data
		Option DB = Option.builder("DB").hasArg().required(true).desc("DB. ").longOpt("DB").build();

		Options options = new Options();
		CommandLineParser parser = new DefaultParser();

		options.addOption(orgsFileOption);
		options.addOption(orgsRNSRFileOption);
		options.addOption(projectsFileOption);
		options.addOption(publicationsFileOption);
		options.addOption(DB);

		String orgsFile = null;
		String orgsRNSRFile = null;
		String projectsFile = null;
		String publicationsFile = null;
		String db = null;
		try {
			commandLine = parser.parse(options, args);

			System.out.println("----------------------------");
			System.out.println("OPTIONS:");

			if(commandLine.hasOption("DB")) {       	
	        	db =commandLine.getOptionValue("DB");
	        	System.out.println("DB name: " + db);
	        } else {
	        	System.out.println("\tDB name not provided. Use the DB option.");
	        	System.exit(1);
	        }
			
			if (commandLine.hasOption("orgsFile")) {
				orgsFile = commandLine.getOptionValue("orgsFile");
				System.out.println("\tScanR organizations data file: " + orgsFile);
			} else {
				System.out.println("\tScanR organizations data file not provided. Use the orgsFile option.");
				System.exit(1);
			}

			if (commandLine.hasOption("orgsRNSRFile")) {
				orgsRNSRFile = commandLine.getOptionValue("orgsRNSRFile");
				System.out.println("\tRNSR organizations data file: " + orgsRNSRFile);
			} else {
				System.out.println("\tRNSR organizations data file not provided. Use the orgsRNSRFile option.");
				System.exit(1);
			}

			if (commandLine.hasOption("projectsFile")) {
				projectsFile = commandLine.getOptionValue("projectsFile");
				System.out.println("\tScanR projects data file: " + projectsFile);
			} else {
				System.out.println("\tScanR projects data file not provided. Use the projectsFile option.");
				System.exit(1);
			}

			if (commandLine.hasOption("publicationsFile")) {
				publicationsFile = commandLine.getOptionValue("publicationsFile");
				System.out.println("\tScanR publications data file: " + publicationsFile);
			} else {
				System.out.println("\tScanR publications data file not provided. Use the publicationsFile option.");
				System.exit(1);
			}						

			System.out.println("----------------------------\n");

		} catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// END: INPUT PARAMETERS --------------------------------------------------------

		// import data
		System.out.println("Starting importing ScanR data...");
		ScanRImporter scanrImporter = new ScanRImporter(orgsFile, orgsRNSRFile, projectsFile, publicationsFile);
		scanrImporter.importData(db);

	}

	public ScanRImporter(String orgsFile, String orgsRNSRFile, String projectsFile, String publicationsFile) {

		// create one CSV reader for each input file
		try {
			
			this.reader = new FileReader(orgsFile);
			this.readerRNSR = new FileReader(orgsRNSRFile);
			this.projectReader = new FileReader(projectsFile);
			this.publicationReader = new FileReader(publicationsFile);
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

	}

	private Source setSource() {
		
		// save data source information
		Source source = new Source();
		source.setLabel(sourceName);
		source.setUrl(this.sourceUrl);
		DateFormat df = new SimpleDateFormat("dd-MM-yyyy");

		Date sourceDate;
		try {
			sourceDate = df.parse(sourceRevisionDate);
			source.setRevisionDate(sourceDate);
		} catch (java.text.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return source;
	}

	private Organization setOrganizationFields(JSONObject data, List<Source> sources) {

		// save organization information into an Organization object
		Organization org = new Organization();

		String label = (String) data.get("label");
		if (label != null) {
			org.setLabel(label);
		}

		String acronym = (String) data.get("acronym");
		if (acronym != null) {
			List<String> acronyms = new ArrayList<>();
			acronyms.add(acronym);
			org.setAcronyms(acronyms);
		}

		if (data.get("creationYear") != null) {
			String creationYear = "" + data.get("creationYear");
			if (!creationYear.equals("")) {
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy");
				Date utilDate = null;
				try {
					utilDate = formatter.parse(creationYear);
				} catch (java.text.ParseException e) {
					e.printStackTrace();
				}
				if (utilDate != null) {
					org.setCreationYear(utilDate);
				}
			}
		}

		String commercialLabel = (String) data.get("commercialLabel");
		if (commercialLabel != null) {
			org.setCommercialLabel(commercialLabel);
		}

		String alias = (String) data.get("alias");
		if (alias != null) {
			org.setAlias(alias);
		}

		JSONObject type = (JSONObject) data.get("type");
		String code = (String) type.get("code_categorie");
		if (code != null) {
			org.setTypeCategoryCode(code);
		}
		String typeLabel = (String) type.get("label");
		if (typeLabel != null) {
			org.setTypeLabel(typeLabel);
		}
		String kind = (String) type.get("kind");
		if (kind != null) {
			org.setTypeKind(kind);
		}
		boolean isPublic = (boolean) type.get("isPublic");
		if (isPublic == true) {
			org.setIsPublic("true");
		} else {
			org.setIsPublic("false");
		}

		JSONObject address = (JSONObject) data.get("address");
		String addressName = (String) address.get("address");
		if (addressName != null) {
			org.setAddress(addressName);
			org.setAddressSources(sources);
		}
		String postcode = (String) address.get("postcode");
		if (postcode != null) {
			org.setPostcode(postcode);
		}
		String city = (String) address.get("city");
		if (city != null) {
			String cleanCity = city.substring(0, 1).toUpperCase() + city.substring(1);
			org.setCity(cleanCity);
		}
		String citycode = (String) address.get("citycode");
		if (citycode != null) {
			org.setCityCode(citycode);
		}
		/*String urbanUnitCode = (String) address.get("urbanUnitCode");
		if (urbanUnitCode != null) {
			org.setUrbanUnitCode(urbanUnitCode);
		}
		String urbanUnit = (String) address.get("urbanUnit");
		if (urbanUnit != null) {
			org.setUrbanUnit(urbanUnit);
		}*/
		JSONObject gps = (JSONObject) address.get("gps");
		Object lat = null;
		if (gps.get("lat") instanceof Double) {
			lat = (Double) gps.get("lat");
		} else {
			if (gps.get("lat") instanceof Long) {
				lat = (Long) gps.get("lat");
			}
		}
		if (lat != null) {
			org.setLat(Float.parseFloat("" + lat));
		}

		Object lon = null;
		if (gps.get("lon") instanceof Double) {
			lon = (Double) gps.get("lon");
		} else {
			if (gps.get("lon") instanceof Long) {
				lon = (Long) gps.get("lon");
			}
		}

		if (lon != null) {
			org.setLon(Float.parseFloat("" + lon));
		}

		JSONObject financePrivate = (JSONObject) data.get("financePrivate");
		String revenueRange = (String) financePrivate.get("revenueRange");
		if (revenueRange != null) {
			org.setFinancePrivateRevenueRange(revenueRange);
		}
		String stringDate = (String) financePrivate.get("date");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

		Date financePrivateDate;
		try {
			financePrivateDate = df.parse(stringDate);
			org.setFinancePrivateDate(financePrivateDate);
		} catch (java.text.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String employees = (String) financePrivate.get("employes");
		if (employees != null) {
			org.setFinancePrivateEmployees(employees);
		}

		org.setCountry("France");
		org.setCountryCode("FR");

		return org;

	}

	private Organization setOrganizationFieldsRNSR(JSONObject data, List<Source> sources) {

		// save secondary organization information into an Organization object
		Organization org = new Organization();

		String label = (String) data.get("label");
		if (label != null) {
			org.setLabel(label);
		}

		String acronym = (String) data.get("acronym");
		if (acronym != null) {
			List<String> acronyms = new ArrayList<>();
			acronyms.add(acronym);
			org.setAcronyms(acronyms);
		}

		String creationYear = "" + data.get("creationYear");
		if (!creationYear.equals("")) {
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy");
			Date utilDate = null;
			try {
				utilDate = formatter.parse(creationYear);
			} catch (java.text.ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (utilDate != null) {
				org.setCreationYear(utilDate);
			}
		}

		JSONObject address = (JSONObject) data.get("address");
		String addressName = (String) address.get("address");
		if (addressName != null) {
			org.setAddress(addressName);
			org.setAddressSources(sources);
		}
		String postcode = (String) address.get("postcode");
		if (postcode != null) {
			org.setPostcode(postcode);
		}
		String city = (String) address.get("city");
		if (city != null) {
			String cleanCity = city.substring(0, 1).toUpperCase() + city.substring(1); 
			org.setCity(cleanCity);
		}
		String citycode = (String) address.get("citycode");
		if (citycode != null) {
			org.setCityCode(citycode);
		}
		/*String urbanUnitCode = (String) address.get("urbanUnitCode");
		if (urbanUnitCode != null) {
			org.setUrbanUnitCode(urbanUnitCode);
		}
		String urbanUnit = (String) address.get("urbanUnit");
		if (urbanUnit != null) {
			org.setUrbanUnit(urbanUnit);
		}*/
		JSONObject gps = (JSONObject) address.get("gps");
		Double lat = (Double) gps.get("lat");
		if (lat != null) {
			org.setLat(Float.parseFloat("" + lat));
		}
		Double lon = (Double) gps.get("lon");
		if (lon != null) {
			org.setLon(Float.parseFloat("" + lon));
		}

		JSONObject finance = (JSONObject) data.get("finance");
		String employees = (String) finance.get("employeesField");
		if (employees != null) {
			org.setFinancePrivateEmployees(employees);
		}
		
		org.setIsPublic("true");

		org.setCountry("France");
		org.setCountryCode("FR");

		return org;

	}

	private List<Leader> setOrganizationLeaders(JSONObject data) {

		// save organization leaders information into Leader objects
		List<Leader> leaders = new ArrayList<Leader>();

		if (data.get("leaders") != null) {
			JSONArray leadersJSON = (JSONArray) data.get("leaders");

			Iterator<JSONObject> it = leadersJSON.iterator();
			while (it.hasNext()) {
				Leader leader = new Leader();

				JSONObject leaderInfo = it.next();

				String firstname = (String) leaderInfo.get("firstname");
				if (firstname != null) {
					leader.setFirstName(firstname);
				}
				String lastname = (String) leaderInfo.get("lastname");
				if (lastname != null) {
					leader.setLastName(lastname);
				}
				String title = (String) leaderInfo.get("title");
				if (title != null) {
					leader.setTitle(title);
				}
				String email = (String) leaderInfo.get("email");
				if (email != null) {
					leader.setEmail(email);
				}
				leaders.add(leader);

			}
		}

		return leaders;
	}

	private List<Link> setOrganizationLinks(JSONObject data) {

		// save organization websites into Link objects
		List<Link> links = new ArrayList<Link>();

		if (data.get("links") != null) {
			JSONArray linksJSON = (JSONArray) data.get("links");

			Iterator<JSONObject> it = linksJSON.iterator();
			while (it.hasNext()) {
				
				JSONObject linkInfo = it.next();
				
				String url = (String) linkInfo.get("url");
				if (url != null) {
					Link link = new Link();
									
					link.setUrl(url);

					String type = (String) linkInfo.get("type");
					if (type != null) {
						link.setType(type);
					}
					String label = (String) linkInfo.get("label");
					if (label != null) {
						
						if(label.equals("Aller sur le site web de la structure")) {
							label = "Access to the web site of the structure";
						}
						if(label.equals("Accéder à la fiche Wikipedia")) {
							label = "Access to the Wikipedia page of the structure";
						}
						link.setLabel(label);
					}
					links.add(link);
				}

			}
		}

		return links;
	}
	
	private Set<OrganizationActivity> setOrganizationActivities(JSONObject data, Map<String, OrganizationActivity> mapActivity, EntityManager em) {

		// save organization activity information into OrganizationActivity objects
		Set<OrganizationActivity> orgActivities = new HashSet<>();

		if (data.get("activities") != null) {
			JSONArray activitiesJSON = (JSONArray) data.get("activities");

			Iterator<JSONObject> it = activitiesJSON.iterator();
			while (it.hasNext()) {
				JSONObject activityInfo = it.next();
				OrganizationActivity activity;
				String code = (String) activityInfo.get("code");
				if (code != null) {
					if (mapActivity.get(code) != null) { 	// organization activity already exists
						activity = mapActivity.get(code);						
					} else { 								// new organization activity
						activity = new OrganizationActivity();
						activity.setCode(code);
						String label = (String) activityInfo.get("label");
						if (label != null) {
							activity.setLabel(label);
						}
						String type = (String) activityInfo.get("type");
						if (type != null) {
							activity.setType(type);
						}
						em.persist(activity);
						mapActivity.put(code, activity);
					}
					
					orgActivities.add(activity);									
				}
			}
		}
		
		return orgActivities;
	
	}

	private OrganizationType setOrganizationType(EntityManager entitymanager, JSONObject data, String attr, Map<String, OrganizationType> mapOrgType) {
		
		// save organization type information into OrganizationType object
		OrganizationType orgType = null;
		
		JSONObject companyType = (JSONObject) data.get(attr);
		String label = (String) companyType.get("label");
		if (label != null && label != "null") {
			if (mapOrgType.get(label) != null) {
				orgType = mapOrgType.get(label);
			} else {
				orgType = new OrganizationType();
				orgType.setLabel(label);
				entitymanager.persist(orgType);
				mapOrgType.put(label, orgType);
			}
		}
		
		return orgType;

	}

	private List<OrganizationRelation> setOrganizationRelations(JSONObject data, boolean RNSR) {

		// save organization relation information into OrganizationRelation objects
		List<OrganizationRelation> relations = new ArrayList<OrganizationRelation>();

		if (data.get("relations") != null) {
			JSONArray relationsJSON = (JSONArray) data.get("relations");

			Iterator<JSONObject> it = relationsJSON.iterator();
			while (it.hasNext()) {
				OrganizationRelation rel = new OrganizationRelation();
				JSONObject relInfo = it.next();

				String url = (String) relInfo.get("url");
				if (url != null) {
					rel.setUrl(url);
				}
				if (!RNSR) {
					String acronym = (String) relInfo.get("acronym");
					if (acronym != null) {
						rel.setAcronym(acronym);
					}
					String logo = (String) relInfo.get("logo");
					if (logo != null) {
						rel.setLogo(logo);
					}
				}
				relations.add(rel);
			}
		}

		return relations;
	}

	private List<Badge> setOrganizationBadges(JSONObject data) {
		
		
		// save organization badge information into Badge objects
		List<Badge> badges = new ArrayList<Badge>();

		if (data.get("badges") != null) {
			JSONArray badgesJSON = (JSONArray) data.get("badges");

			Iterator<JSONObject> it = badgesJSON.iterator();
			while (it.hasNext()) {
				Badge badge = new Badge();
				JSONObject badgeInfo = it.next();

				String label = (String) badgeInfo.get("label");
				if (label != null) {
					badge.setLabel(label);
				}
				String code = (String) badgeInfo.get("code");
				if (code != null) {
					badge.setCode(code);
				}

				badges.add(badge);
			}
		}

		return badges;
	}

	private List<SpinoffFrom> setOrganizationSpinOffs(JSONObject data, String attrId, String attrLabel) {
		
		// save organization spin-off information into SpinoffFrom objects
		List<SpinoffFrom> spinOffs = new ArrayList<SpinoffFrom>();

		if (data.get("spinoffFrom") != null) {
			JSONArray spinoffJSON = (JSONArray) data.get("spinoffFrom");

			Iterator<JSONObject> it = spinoffJSON.iterator();
			while (it.hasNext()) {
				SpinoffFrom spinoff = new SpinoffFrom();
				JSONObject spinoffInfo = it.next();

				String label = (String) spinoffInfo.get(attrLabel);
				if (label != null) {
					spinoff.setLabel(label);
				}

				spinOffs.add(spinoff);
			}
		}

		return spinOffs;

	}

	private List<OrganizationIdentifier> setOrganizationIdentifiers(JSONObject data, Organization org, String attrId) {

		// save organization identifier information into OrganizationIdentifier object
		List<OrganizationIdentifier> orgIds = new ArrayList<OrganizationIdentifier>();

		OrganizationIdentifier orgId = new OrganizationIdentifier();
		String orgIdentifier = (String) data.get(attrId);
		orgId.setIdentifier(orgIdentifier);
		orgId.setProvenance(sourceName);
		orgId.setIdentifierName("ScanR url");
		orgId.setOrganization(org);
		orgId.setLink("https://scanr.enseignementsup-recherche.gouv.fr/structure/" + orgIdentifier);
		orgIds.add(orgId);

		return orgIds;

	}
	
	public static String getOrgIds(List<OrganizationIdentifier> orgIds) {

		String orgId = null;
		
		if (orgIds != null) {
			List<String> ids = new ArrayList<>();
			for (OrganizationIdentifier oi: orgIds) {
				ids.add(oi.getIdentifierName() + "_" + oi.getIdentifier());
			}
			orgId = String.join(",", ids);
		}
		
		return orgId;
	}

	public Map<String, Organization> importOrganizations(EntityManager entitymanager, List<Source> sources, Map<String, OrganizationActivity> mapActivity, Map<String, OrganizationType> mapOrgType) {

		JSONParser parser = new JSONParser();
		int count = 0;
		Map<String, Organization> mapOrgs = new HashMap<String, Organization>();			

		entitymanager.getTransaction().begin();

		try {

			// read JSON file
			Object fileObj = parser.parse(reader);
			JSONArray orgsArray = (JSONArray) fileObj;
			Iterator<JSONObject> it = orgsArray.iterator();
			
			// loop over JSON rows
			while (it.hasNext()) {
				if ((count % 1000) == 0) {
					System.out.println(count);
				}
				count++;

				JSONObject orgData = it.next();	// JSON data row

				// set main organization fields
				Organization org = setOrganizationFields(orgData, sources);				

				// leaders
				List<Leader> leaders = setOrganizationLeaders(orgData);
				for (Leader l : leaders) {
					l.setSources(sources);
					entitymanager.persist(l);
				}
				if (leaders.size() > 0) {
					org.setLeaders(leaders);
				}

				// connect the source to the related organization
				if (org != null) {
					org.setSources(sources);
				}

				// organization websites
				List<Link> links = setOrganizationLinks(orgData);
				for (Link l : links) {
					l.setSources(sources);
					entitymanager.persist(l);
				}
				if (links.size() > 0) {
					org.setLinks(links);
				}

				// organization activities
				Set<OrganizationActivity> setActivities = setOrganizationActivities(orgData, mapActivity, entitymanager);
				List<OrganizationActivity> activities = new ArrayList<>(setActivities);
				if (activities.size() > 0) {
					org.setActivities(activities);
				}

				// organization type
				OrganizationType orgType = setOrganizationType(entitymanager, orgData, "companyType", mapOrgType);
				if (orgType != null) {
					org.setOrganizationType(orgType);
				}

				// organization relations
				List<OrganizationRelation> relations = setOrganizationRelations(orgData, false);
				for (OrganizationRelation r : relations) {
					entitymanager.persist(r);
				}
				if (relations.size() > 0) {
					org.setOrganizationRelations(relations);
				}

				// organization badges
				List<Badge> badges = setOrganizationBadges(orgData);
				for (Badge b : badges) {
					entitymanager.persist(b);
				}
				if (badges.size() > 0) {
					org.setBadges(badges);
				}

				// organization spinoffFrom
				List<SpinoffFrom> spinOffs = setOrganizationSpinOffs(orgData, "id", "label");
				for (SpinoffFrom s : spinOffs) {
					entitymanager.persist(s);
				}
				if (spinOffs.size() > 0) {
					org.setSpinoffFroms(spinOffs);
				}

				// set organization identifiers
				if (org != null) {
					List<OrganizationIdentifier> orgIdentifiers = setOrganizationIdentifiers(orgData, org, "id");
					for (OrganizationIdentifier orgIdentifier : orgIdentifiers) {
						entitymanager.persist(orgIdentifier);
					}				
					
					// set unique identifier
					String identifier = sourceName + "::" + getOrgIds(orgIdentifiers);
					OrganizationIdentifier orgId = new OrganizationIdentifier();
					orgId.setIdentifier(identifier);
					orgId.setProvenance(sourceName);
					orgId.setIdentifierName("lid");
					orgId.setOrganization(org);
					orgId.setVisibility(false);
					entitymanager.persist(orgId);
				}
				
				
				// set organization extra fields
				/*
				 * if (org != null) { List<OrganizationExtraField> orgExtraFields =
				 * setOrganizationExtraFields(orgData, org); for (OrganizationExtraField
				 * orgExtraField: orgExtraFields) { entitymanager.persist(orgExtraField); } }
				 */

				if (org != null) {
					entitymanager.persist(org);
				}
				String id = (String) orgData.get("id");
				mapOrgs.put(id, org);

			}

			reader.close();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		entitymanager.getTransaction().commit();

		return mapOrgs;

	}

	private Map<String, Organization> importRNSROrganizations(EntityManager entitymanager, List<Source> sources,
			Map<String, Organization> scanrMapOrgs, Map<String, OrganizationActivity> mapActivity, Map<String, OrganizationType> mapOrgType) {

		JSONParser parser = new JSONParser();
		int count = 0;
		Map<String, Organization> mapOrgs = new HashMap<String, Organization>();
		Map<String, List<Organization>> parentsRNSR = new HashMap<String, List<Organization>>();
		Map<String, List<Organization>> parents = new HashMap<String, List<Organization>>();

		entitymanager.getTransaction().begin();

		try {

			// read JSON file
			Object fileObj = parser.parse(readerRNSR);
			JSONArray orgsArray = (JSONArray) fileObj;
			Iterator<JSONObject> it = orgsArray.iterator();
			
			// loop over JSON rows
			while (it.hasNext()) {
				if ((count % 1000) == 0) {
					System.out.println(count);
				}
				count++;

				// JSON data row
				JSONObject orgData = it.next();

				// set main organization fields
				Organization org = setOrganizationFieldsRNSR(orgData, sources);
				org.setAddressSources(sources);

				// leaders
				List<Leader> leaders = setOrganizationLeaders(orgData);
				for (Leader l : leaders) {
					l.setSources(sources);
					entitymanager.persist(l);
				}
				if (leaders.size() > 0 && org != null) {
					org.setLeaders(leaders);
				}

				// connect the source to the related organization
				if (org != null) {
					org.setSources(sources);
				}

				// organization links
				List<Link> links = setOrganizationLinks(orgData);
				for (Link l : links) {
					l.setSources(sources);
					entitymanager.persist(l);
				}
				if (links.size() > 0 && org != null) {
					org.setLinks(links);
				}

				// organization activities
				Set<OrganizationActivity> setActivities = setOrganizationActivities(orgData, mapActivity, entitymanager);
				List<OrganizationActivity> activities = new ArrayList<>(setActivities);
				if (activities.size() > 0 && org != null) {
					org.setActivities(activities);
				}

				// organization type
				OrganizationType orgType = setOrganizationType(entitymanager, orgData, "type", mapOrgType);
				if (orgType != null) {
					entitymanager.persist(orgType);
					org.setOrganizationType(orgType);
				}

				// organization relations
				List<OrganizationRelation> relations = setOrganizationRelations(orgData, true);
				for (OrganizationRelation r : relations) {
					entitymanager.persist(r);
				}
				if (relations.size() > 0 && org != null) {
					org.setOrganizationRelations(relations);
				}

				// organization badges
				List<Badge> badges = setOrganizationBadges(orgData);
				for (Badge b : badges) {
					entitymanager.persist(b);
				}
				if (badges.size() > 0 && org != null) {
					org.setBadges(badges);
				}

				// organization spinoffFrom
				List<SpinoffFrom> spinOffs = setOrganizationSpinOffs(orgData, "idCompany", "labelCompany");
				for (SpinoffFrom s : spinOffs) {
					entitymanager.persist(s);
				}
				if (spinOffs.size() > 0 && org != null) {
					org.setSpinoffFroms(spinOffs);
				}

				// set organization identifiers
				if (org != null) {
					List<OrganizationIdentifier> orgIdentifiers = setOrganizationIdentifiers(orgData, org, "id");
					for (OrganizationIdentifier orgIdentifier : orgIdentifiers) {
						entitymanager.persist(orgIdentifier);
					}					
					
					// set unique identifier
					String identifier = sourceName + "::" + getOrgIds(orgIdentifiers);
					OrganizationIdentifier orgId = new OrganizationIdentifier();
					orgId.setIdentifier(identifier);
					orgId.setProvenance(sourceName);
					orgId.setIdentifierName("lid");
					orgId.setOrganization(org);
					orgId.setVisibility(false);
					entitymanager.persist(orgId);
				}

				// save hierarchical (parent-children) relationships
				if (orgData.get("parent") != null) {
					JSONArray parentJSON = (JSONArray) orgData.get("parent");
					Iterator<JSONObject> ite = parentJSON.iterator();
					while (ite.hasNext()) {
						JSONObject parent = ite.next();
						String idParent = (String) parent.get("id");

						List<Organization> parentOrgs = parentsRNSR.get(idParent);
						if (parentOrgs != null) {
							parentsRNSR.get(idParent).add(org);
						} else {
							List<Organization> val = new ArrayList<Organization>();
							val.add(org);
							parentsRNSR.put(idParent, val);
						}
					}
				}

				if (orgData.get("institutions") != null) {
					JSONArray parentJSON = (JSONArray) orgData.get("institutions");
					Iterator<JSONObject> ite = parentJSON.iterator();
					while (ite.hasNext()) {
						JSONObject parent = ite.next();
						String idParent = (String) parent.get("id");

						List<Organization> parentOrgs = parents.get(idParent);
						if (parentOrgs != null) {
							parents.get(idParent).add(org);
						} else {
							List<Organization> val = new ArrayList<Organization>();
							val.add(org);
							parents.put(idParent, val);
						}
					}
				}

				String id = (String) orgData.get("id");
				mapOrgs.put(id, org);
				 

				if (org != null) {
					entitymanager.persist(org);
				}

			}

			readerRNSR.close();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}

		Map<Organization, Set<Organization>> relOrgs = new HashMap<Organization, Set<Organization>>();

		// loop over parentsRNSR map in order to connect RNSR organizations by
		// parent-child relationship
		for (String parentId : parentsRNSR.keySet()) {
			Organization parentOrg = mapOrgs.get(parentId);
			if (parentOrg != null) {
				List<Organization> orgs = parentsRNSR.get(parentId);
				List<Organization> dedupOrgs = new ArrayList<Organization>();
				List<String> orgsNames = new ArrayList<String>();
				for (Organization child : orgs) {
					if (!orgsNames.contains(child.getLabel())) {
						dedupOrgs.add(child);
						orgsNames.add(child.getLabel());
					}
				}
				parentOrg.setChildrenOrganizations(dedupOrgs);
				entitymanager.merge(parentOrg);
			}
		}

		// loop over parents map in order to connect RNSR-ScanR organizations by
		// parent-child relationship
		for (String parentId : parents.keySet()) {
			Organization parentOrg = scanrMapOrgs.get(parentId);
			if (parentOrg != null) {
				List<Organization> orgs = parents.get(parentId);
				List<Organization> dedupOrgs = new ArrayList<Organization>();
				List<String> orgsNames = new ArrayList<String>();
				for (Organization child : orgs) {
					if (!orgsNames.contains(child.getLabel())) {
						dedupOrgs.add(child);
						orgsNames.add(child.getLabel());
					}
				}
				parentOrg.setChildrenOrganizations(dedupOrgs);

				entitymanager.merge(parentOrg);
			}
		}
		
		entitymanager.getTransaction().commit();

		return mapOrgs;
	}

	private Project setProjectFields(JSONObject data) {

		// save project information into Project object
		Project project = new Project();

		String label = (String) data.get("label");
		if (label != null) {
			label = label.replace("\n", "");
			label = label.replace("\t", "");
			project.setLabel(label);
		}

		String acronym = (String) data.get("acronym");
		if (acronym != null) {
			project.setAcronym(acronym);
		}

		if (data.get("description") != null) {
			String description = (String) data.get("description");
			if (description != null) {
				project.setDescription(description);
			}
		}

		if (data.get("duration") != null) {
			Long duration = (Long) data.get("duration");
			project.setDuration(duration + "");
		}

		Object budget = null;
		if (data.get("budget") instanceof Double) {
			budget = (Double) data.get("budget");
		} else {
			if (data.get("budget") instanceof Long) {
				budget = (Long) data.get("budget");
			}
		}
		if (budget != null) {
			project.setBudget(budget + "");
		}

		/*
		 * if(data.get("budget") != null) { Long budget = (Long) data.get("budget");
		 * project.setBudget(budget+""); }
		 */

		if (data.get("call") != null) {
			String call = (String) data.get("call");
			project.setCallId(call);
		}

		if (data.get("callLabel") != null) {
			String callLabel = (String) data.get("callLabel");
			project.setCallLabel(callLabel);
		}

		if (data.get("url") != null) {
			String url = (String) data.get("url");
			project.setUrl(url);
		}

		if (data.get("date_debut") != null) {
			String date_debut = (String) data.get("date_debut");

			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			Date utilDate = null;
			try {
				utilDate = formatter.parse(date_debut);
			} catch (java.text.ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (utilDate != null) {
				project.setStartDate(utilDate);
			}
		}

		if (data.get("type") != null) {
			String type = (String) data.get("type");
			project.setType(type);
		}

		if (data.get("year") != null) {
			Long year = (Long) data.get("year");
			project.setYear(year + "");
		}

		if (data.get("month") != null) {
			Long month = (Long) data.get("month");
			project.setMonth(month + "");
		}

		return project;
	}

	private List<Thematic> setProjectThemes(JSONObject data, Map<String, Thematic> thematicAlreadyStored) {

		// save project theme information into Thematic objects
		List<Thematic> themes = new ArrayList<Thematic>();
		Set<String> uniqueThemes = new HashSet<>();

		if (data.get("themes") != null) {
			JSONArray themesJSON = (JSONArray) data.get("themes");
			int length = themesJSON.size();
			if (length > 0) {
				for (int i = 0; i < length; i++) {
					Thematic theme = null;
					String label = (String) themesJSON.get(i);
					if (label != null && !label.equals("")) {
						if (thematicAlreadyStored.get(label) != null) { // thematic already inserted
							theme = thematicAlreadyStored.get(label);
						} else { 										// new thematic
							theme = new Thematic();
							theme.setLabel(label);
							thematicAlreadyStored.put(label, theme);
						}
						// insert in output list only unique thematics
						if (theme != null) {
							boolean insert = true;
							for (String lab : uniqueThemes) {
								if (lab.equals(label)) {
									insert = false;
									break;
								}
							}
							if (insert == true) {
								themes.add(theme);
								uniqueThemes.add(label);
							}
						}
					}

				}
			}
		}

		return themes;
	}

	private List<ProjectIdentifier> setProjectIdentifiers(JSONObject data, Project prj) {
		
		// save project identifier information into ProjectIdentifier object
		List<ProjectIdentifier> prjIds = new ArrayList<ProjectIdentifier>();

		ProjectIdentifier prjId = new ProjectIdentifier();
		String prjIdentifier = (String) data.get("id");
		prjId.setIdentifier(prjIdentifier);
		prjId.setProvenance(sourceName);
		prjId.setIdentifierName("id");
		prjId.setProject(prj);
		prjIds.add(prjId);

		return prjIds;

	}

	private void importProjects(EntityManager entitymanager, List<Source> sources) {
		JSONParser parser = new JSONParser();
		int count = 0;
		Map<String, List<Project>> orgProjs = new HashMap<String, List<Project>>();
		Map<String, Thematic> thematicAlreadyStored = new HashMap<>();
		// DELETE DISCONNECTED PROJECTS
		//int numProjectsNotConnected = 0;			

		// retrieve ScanR organizations
		Map<String, Organization> orgsMap = retrieveOrganizations(entitymanager, sources.get(0));

		entitymanager.getTransaction().begin();

		try {

			Object fileObj = parser.parse(projectReader);
			JSONArray projectsArray = (JSONArray) fileObj;
			Iterator<JSONObject> it = projectsArray.iterator();
			while (it.hasNext()) {
				if ((count % 1000) == 0) {
					System.out.println(count);
				}
				count++;

				// DELETE DISCONNECTED PROJECTS
				//boolean persitProject = true;	

				JSONObject projectData = it.next();

				// set main organization fields
				Project project = setProjectFields(projectData);

				// set project themes
				List<Thematic> themes = setProjectThemes(projectData, thematicAlreadyStored);
				if (themes.size() > 0 && project != null) {
					project.setThematics(themes);
				}

				// connect the source to the related project
				if (project != null) {
					project.setSources(sources);
				}				

				// loop over organizations connected with current project
				// duplicated organizations may exist!!
				List<ExternalParticipant> extParticipants = new ArrayList<>();
				if (projectData.get("structures") != null) {
					JSONArray projectOrgsJSON = (JSONArray) projectData.get("structures");

					// DELETE DISCONNECTED PROJECTS
					/*if (projectOrgsJSON.size() == 0) {
						persitProject = false;
					}*/
					
					Iterator<JSONObject> ite = projectOrgsJSON.iterator();
					Set<String> orgIdsConnectedToProject = new HashSet<>();
					
					// DELETE DISCONNECTED PROJECTS
					//int numOrgsWithIdConnectedWithProject = 0;
					
					while (ite.hasNext()) {
						JSONObject projectOrgJSON = ite.next();
						if (projectOrgJSON.get("id") != null) {
							String orgId = (String) projectOrgJSON.get("id");
							orgIdsConnectedToProject.add(orgId);
							/*try {
							      int id = Integer.parseInt(orgId);
							      
							      // DELETE DISCONNECTED PROJECTS
							      //numOrgsWithIdConnectedWithProject+=1;
							      
							} catch (NumberFormatException e) {
								System.out.println("Found organization id that is not a number: " + orgId);
							}	*/						
						} else {
							ExternalParticipant ep = new ExternalParticipant();
							ep.setLabel((String) projectOrgJSON.get("label"));
							ep.setUrl((String) projectOrgJSON.get("url"));
							entitymanager.persist(ep);
							extParticipants.add(ep);
						}
					}
					
					// DELETE DISCONNECTED PROJECTS
					/*if (numOrgsWithIdConnectedWithProject==0) {
						numProjectsNotConnected+=1;
						persitProject = false;
					}*/															
					
					// loop over deduplicated organizations and for each organization I add the
					// current project to its project list
					for (String orgId : orgIdsConnectedToProject) {

						List<Project> prjs = orgProjs.get(orgId);
						if (prjs != null) {
							if (project != null) {
								orgProjs.get(orgId).add(project);
							}
						} else {
							if (project != null) {
								List<Project> val = new ArrayList<Project>();
								val.add(project);
								orgProjs.put(orgId, val);
							}
						}
					}
				}/* else {						// DELETE DISCONNECTED PROJECTS
					persitProject = false;
				}*/
				
				// DELETE DISCONNECTED PROJECTS
				// set organization identifiers
				/*if (persitProject && project != null) {
					List<ProjectIdentifier> projectIdentifiers = setProjectIdentifiers(projectData, project);
					for (ProjectIdentifier projectIdentifier : projectIdentifiers) {
						entitymanager.persist(projectIdentifier);
					}
				}*/
				
				// DELETE DISCONNECTED PROJECTS
				// if (persitProject && project != null) {
				
				if (project != null) {
					List<ProjectIdentifier> projectIdentifiers = setProjectIdentifiers(projectData, project);
					// create ProjectIdentifier tuples in the DB
					for (ProjectIdentifier projectIdentifier : projectIdentifiers) {
						entitymanager.persist(projectIdentifier);
					}
					
					if (extParticipants.size() > 0) {
						project.setExternalParticipants(extParticipants);
					}

					// DELETE DISCONNECTED PROJECTS
					/*if (persitProject) {
						entitymanager.persist(project);
					}*/
					entitymanager.persist(project);
				}

			}

			projectReader.close();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}

		entitymanager.getTransaction().commit();

		// loop over map <orgId -> related project list> in order to set to the projects
		// to related organization
		// in this phase I ony consider organization from enterprise.json
		entitymanager.getTransaction().begin();

		for (String orgId : orgProjs.keySet()) {
			Organization org = orgsMap.get(orgId);
			if (org != null) {
				List<Project> relatedPrjs = orgProjs.get(orgId);
				if (relatedPrjs != null && relatedPrjs.size() > 0) {
					System.out.println("Connect org " + org.getLabel() + "[" + orgId + "] with " + relatedPrjs.size() + " projects.");
					org.setProjects(relatedPrjs);
					entitymanager.merge(org);
				}
			}
		}

		entitymanager.getTransaction().commit();

	}

	private Map<String, Organization> retrieveOrganizations(EntityManager entitymanager, Source source) {

		entitymanager.getTransaction().begin();

		// retrieve organizations
		System.out.println("Starting retrieving organizations...");
		Query queryOrg = entitymanager.createQuery("Select o FROM Organization o WHERE :id MEMBER OF o.sources");
		queryOrg.setParameter("id", source);
		List<Organization> orgs = queryOrg.getResultList();		

		Map<String, Organization> orgMap = new HashMap<String, Organization>();
		int num_org = 0;
		for (Organization org : orgs) {
			if ((num_org % 1000) == 0) {
				System.out.println(num_org);
			}
			List<OrganizationIdentifier> orgIds = org.getOrganizationIdentifiers();
			String id = null;
			for (OrganizationIdentifier orgId : orgIds) {
				if (orgId.getIdentifierName().equals("ScanR url")) {
					id = orgId.getIdentifier();
					break;
				}
			}
			if (id != null) {
				orgMap.put(id, org);
				num_org += 1;
			} else {
				System.out.println("Identifier non trovato");
			}
		}

		System.out.println("Retrieved " + num_org + " organizations");
		entitymanager.getTransaction().commit();

		return orgMap;
	}

	private Publication setPublicationFields(JSONObject data) {

		// save publication information into Publication object
		Publication publication = new Publication();

		String title = (String) data.get("title");
		if (title != null) {
			publication.setTitle(title);
		}

		String descr = (String) data.get("summary");
		if (descr != null) {
			publication.setDescription(descr);
		}

		if (data.get("publicationDate") != null) {
			String publicationDate = (String) data.get("publicationDate");

			SimpleDateFormat formatter = new SimpleDateFormat("yyyy");
			Date utilDate = null;
			try {
				utilDate = formatter.parse(publicationDate);
			} catch (java.text.ParseException e) {
				e.printStackTrace();
			}
			if (utilDate != null) {
				publication.setPublicationDate(utilDate);
			}
		}

		if (data.get("type") != null) {
			String type = (String) data.get("type");
			if (type != null) {
				publication.setType(type);
			}
		}

		if (data.get("link") != null) {
			String url = (String) data.get("link");
			publication.setUrl(url);
		}

		String subtitle = (String) data.get("subtitle");
		if (subtitle != null) {
			publication.setSubtitle(subtitle);
		}

		return publication;
	}

	private List<PublicationIdentifier> setPublicationIdentifiers(JSONObject data, Publication publication) {
		
		// save publication identifier information into PublicationIdentifier 
		List<PublicationIdentifier> pubIds = new ArrayList<PublicationIdentifier>();

		PublicationIdentifier pubId = new PublicationIdentifier();

		String pubIdentifier = (String) data.get("id");
		pubId.setIdentifier(pubIdentifier);
		pubId.setProvenance(sourceName);
		pubId.setIdentifierName("id");
		pubId.setPublication(publication);
		pubIds.add(pubId);

		JSONObject externalIdentifiers = (JSONObject) data.get("identifiers");
		if (externalIdentifiers != null) {

			String thesisId = (String) externalIdentifiers.get("thesesfr");
			if (thesisId != null) {
				PublicationIdentifier thesisPubId = new PublicationIdentifier();
				thesisPubId.setIdentifier(thesisId);
				thesisPubId.setProvenance(sourceName);
				thesisPubId.setIdentifierName("thesesfr");
				thesisPubId.setPublication(publication);
				pubIds.add(thesisPubId);

			}

			String patentId = (String) externalIdentifiers.get("patent");
			if (patentId != null) {
				PublicationIdentifier patentPubId = new PublicationIdentifier();
				patentPubId.setIdentifier(patentId);
				patentPubId.setProvenance(sourceName);
				patentPubId.setIdentifierName("patent");
				patentPubId.setPublication(publication);
				pubIds.add(patentPubId);

			}
		}

		return pubIds;

	}

	private List<Thematic> setPublicationThematics(EntityManager entitymanager, JSONObject data,
			Map<String, Thematic> thematicsAlreadyInserted) {

		// save publication thematic information into Thematic objects
		List<Thematic> thematics = new ArrayList<>();

		JSONArray thematicsData = (JSONArray) data.get("thematics");
		if (thematicsData != null) {
			Iterator<JSONObject> it = thematicsData.iterator();

			while (it.hasNext()) {
				JSONObject thematicData = it.next();

				Thematic thematic = null;

				String type = (String) thematicData.get("type");
				String code = (String) thematicData.get("code");
				String label = (String) thematicData.get("label");

				if (type != null || code != null || label != null) {
					// check if exists the same thematic
					String key = type + "_" + code + "_" + label;
					if (thematicsAlreadyInserted.get(key) != null) { 	// thematic already exists
						thematic = thematicsAlreadyInserted.get(key);
					} else { 											// thematic does not exist
						thematic = new Thematic();

						if (type != null) {
							thematic.setClassificationSystem(type);
						}

						if (code != null) {
							thematic.setCode(code);
						}

						if (label != null) {
							thematic.setLabel(label);
						}

						thematicsAlreadyInserted.put(key, thematic);
					}
				}

				// adding only unique thematics (inside the considered publication)
				if (thematic != null) {
					boolean insert = true;
					for (Thematic th : thematics) {
						if (th.getClassificationSystem().equals(thematic.getClassificationSystem())
								&& th.getCode().equals(thematic.getCode())
								&& th.getLabel().equals(thematic.getLabel())) {
							insert = false;
							break;
						}
					}
					if (insert == true) {
						thematics.add(thematic);
					}
				}
			}

		}

		return thematics;

	}

	private List<Organization> connectPublicationWithOrganizations(EntityManager entitymanager, JSONObject data,
			Publication publication, Map<String, Organization> orgsMap) {

		// connect publications with organizations
		List<Organization> orgs = new ArrayList<>();

		JSONArray structures = (JSONArray) data.get("structures");
		if (structures != null) {
			int length = structures.size();
			if (length > 0) {
				for (int i = 0; i < length; i++) {

					String structureId = (String) structures.get(i);
					if (structureId != null) {
						// search structure inside orgsMap
						Organization connectOrg = orgsMap.get(structureId);
						if (connectOrg != null) {
							List<Publication> oldOrgPubs = connectOrg.getPublications();
							if (oldOrgPubs != null) {
								oldOrgPubs.add(publication);
								connectOrg.setPublications(oldOrgPubs);
							} else {
								List<Publication> newOrgPubs = new ArrayList<>();
								newOrgPubs.add(publication);
								connectOrg.setPublications(newOrgPubs);
							}
							entitymanager.merge(connectOrg);
							orgs.add(connectOrg);
						} else {
							// System.out.println("Connected org not found");
						}
					}
				}
			}
		}

		return orgs;
	}

	private List<Organization> importPublications(EntityManager entitymanager, List<Source> sources) {

		JSONParser parser = new JSONParser();
		int count = 0;

		List<Organization> orgs = new ArrayList<>();
		Map<String, Thematic> thematicsAlreadyInserted = new HashMap<>();

		// retrieve ScanR organizations
		Map<String, Organization> orgsMap = retrieveOrganizations(entitymanager, sources.get(0));

		entitymanager.getTransaction().begin();

		try {

			// read JSON file
			Object fileObj = parser.parse(publicationReader);
			JSONArray publicationsArray = (JSONArray) fileObj;
			Iterator<JSONObject> it = publicationsArray.iterator();
			
			// loop over JSON rows
			while (it.hasNext()) {
				if ((count % 100) == 0) {
					System.out.println(count);
				}
				count++;

				// JSON data row
				JSONObject pubData = it.next();

				// set main organization fields
				Publication publication = setPublicationFields(pubData);

				// set publication identifiers
				if (publication != null) {
					List<PublicationIdentifier> publicationIdentifiers = setPublicationIdentifiers(pubData,
							publication);
					for (PublicationIdentifier publicationIdentifier : publicationIdentifiers) {
						entitymanager.persist(publicationIdentifier);
					}
				}

				// connect the source to the related publication
				if (publication != null) {
					publication.setSources(sources);
				}

				// set publication thematics
				List<Thematic> thematics = setPublicationThematics(entitymanager, pubData, thematicsAlreadyInserted);
				for (Thematic t : thematics) {
					entitymanager.persist(t);
				}
				if (thematics.size() > 0 && publication != null) {
					publication.setThematics(thematics);
				}

				// connect publication with organizations
				if (publication != null) {
					List<Organization> newOrgs = connectPublicationWithOrganizations(entitymanager, pubData,
							publication, orgsMap);
					for (Organization org : newOrgs) {
						orgs.add(org);
					}
				}

				if (publication != null) {
					entitymanager.persist(publication);
				}
			}

			publicationReader.close();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}

		entitymanager.getTransaction().commit();

		return orgs;

	}

	public void importData(String db) {

		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();

		// get or create ScanR source
		// ---------------------------------------------------------------
		entitymanager.getTransaction().begin();
		Query query = entitymanager.createQuery("Select s FROM Source s where s.label = :label");
		query.setParameter("label", sourceName);
		List<Source> scanRSource = query.getResultList();
		Source source = null;
		if (scanRSource.size() > 0) {
			source = scanRSource.get(0);
			System.out.println("Retrieved " + source.getLabel() + " source");
		} else {
			source = setSource();
			entitymanager.persist(source);
			System.out.println("Created " + source.getLabel() + " source");
		}
		List<Source> sources = new ArrayList<Source>();
		sources.add(source);
		entitymanager.getTransaction().commit();
		// ----------------------------------------------------------------------------------------

		// import organizations
		Map<String, OrganizationActivity> mapActivity = new HashMap<>();
		System.out.println("Starting importing organizations");
		Map<String, OrganizationType> mapOrgType = new HashMap<>();
		Map<String, Organization> mapOrgs = importOrganizations(entitymanager, sources, mapActivity, mapOrgType);
		Map<String, Organization> mapOrgsRNSR = importRNSROrganizations(entitymanager, sources, mapOrgs, mapActivity, mapOrgType);
		entitymanager.close();
		emfactory.close();

		emfactory = Persistence.createEntityManagerFactory(db);
		entitymanager = emfactory.createEntityManager();

		entitymanager.getTransaction().begin();
		query = entitymanager.createQuery("Select s FROM Source s where s.label = :label");
		query.setParameter("label", sourceName);
		scanRSource = query.getResultList();
		source = scanRSource.get(0);
		System.out.println("Retrieved " + source.getLabel() + " source");
		sources = new ArrayList<Source>();
		sources.add(source);
		entitymanager.getTransaction().commit();

		// import projects
		System.out.println("Starting importing projects");
		importProjects(entitymanager, sources);

		// import publications
		System.out.println("Starting importing publications");
		List<Organization> orgsTest = importPublications(entitymanager, sources);
		
		System.out.println("Importer completed.");

		entitymanager.close();
		emfactory.close();

	}

}
