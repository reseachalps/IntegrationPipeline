package it.unimore.alps.sources.openaire;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
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

import com.opencsv.bean.CsvToBeanBuilder;

import it.unimore.alps.idgenerator.LocalIdentifierGenerator;
import it.unimore.alps.sources.cordis.CordisOrganization;
import it.unimore.alps.sources.cordis.CordisProject;
import it.unimore.alps.sql.model.ExternalParticipant;
import it.unimore.alps.sql.model.Link;
import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationExtraField;
import it.unimore.alps.sql.model.OrganizationIdentifier;
import it.unimore.alps.sql.model.Person;
import it.unimore.alps.sql.model.PersonIdentifier;
import it.unimore.alps.sql.model.Project;
import it.unimore.alps.sql.model.ProjectExtraField;
import it.unimore.alps.sql.model.ProjectIdentifier;
import it.unimore.alps.sql.model.Publication;
import it.unimore.alps.sql.model.PublicationIdentifier;
import it.unimore.alps.sql.model.Source;

public class OpenAireImporter {

	public static void main(String[] args) {

		CommandLine commandLine;
		Option orgFolderOption = Option.builder("orgFolder").hasArg().required(true)
				.desc("The file that contains organization data. ").longOpt("organizationFolder").build();
		//
		Option orgPrefixOption = Option.builder("orgPrefix").hasArg().required(true)
				.desc("The prefix that identify files  that contains organization data. ").longOpt("organizationPrefix")
				.build();

		Option orgCountryOption = Option.builder("country").hasArg().required(true)
				.desc("The file that contains organization data. ").longOpt("organizationCountry").build();

		Option projectFolderOption = Option.builder("prjFolder").hasArg().required(true)
				.desc("The file that contains project data.").longOpt("projectFolder").build();

		Option projecPrefixOption = Option.builder("prjPrefix").hasArg().required(true)
				.desc("The prefix that identify files that contains project data.").longOpt("projectPrefix").build();

		Option prjFileH2020Option = Option.builder("prjFileH2020").hasArg().required(true)
				.desc("The file that contains the H2020 Project.").longOpt("ProjectFileH2020").build();

		Option prjFileFP7Option = Option.builder("prjFileFP7").hasArg().required(true)
				.desc("The file that contains the FP7 Project.").longOpt("ProjectFileFP7").build();

		Option orgFileH2020Option = Option.builder("orgFileH2020").hasArg().required(true)
				.desc("The file that contains the H2020 Organizations.").longOpt("OrganizationFileH2020").build();

		Option orgFileFP7Option = Option.builder("orgFileFP7").hasArg().required(true)
				.desc("The file that contains the FP7 Organizations.").longOpt("OrganizationFileFP7").build();

		Option dbOption = Option.builder("db").hasArg().required(true)
				.desc("The name of the database of the data ingestion.").longOpt("DB").build();

		Option pubOpenAireFolderOption = Option.builder("pubFolderOpenAire").hasArg().required(true)
				.desc("The folder that contains the Openaire Publications.").longOpt("PubFolderOpenAire").build();

		Options options = new Options();

		options.addOption(orgFolderOption);
		options.addOption(orgPrefixOption);
		options.addOption(orgCountryOption);
		options.addOption(projectFolderOption);

		options.addOption(projecPrefixOption);
		options.addOption(prjFileH2020Option);
		options.addOption(prjFileFP7Option);
		options.addOption(orgFileH2020Option);
		options.addOption(orgFileFP7Option);
		options.addOption(dbOption);
		options.addOption(pubOpenAireFolderOption);

		CommandLineParser parser = new DefaultParser();

		String orgFolder = null;
		String orgPrefix = null;
		String orgCountry = null;
		//
		//
		String projectFolder = null;
		String projectPrefix = null;
		String bindingFile = null;
		String entityType = null;

		String prjFileH2020 = null;
		String prjFileFP7 = null;
		String orgFileH2020 = null;
		String orgFileFP7 = null;
		String db = null;
		String publicationFolder = null;
		boolean header = true;

		boolean DEBUG = false;

		try {
			commandLine = parser.parse(options, args);

			if (commandLine.hasOption("orgFolder")) {
				orgFolder = commandLine.getOptionValue("orgFolder");
			} else {
				System.out.println("Organization folder not provided. Use the orgFolder option.");
				System.exit(0);
			}
			System.out.println("Org folder: " + orgFolder);

			if (commandLine.hasOption("orgPrefix")) {
				orgPrefix = commandLine.getOptionValue("orgPrefix");
			} else {
				System.out.println("Organization prefix not provided. Use the orgFolder option.");
				System.exit(0);
			}

			System.out.println("Org prefix: " + orgPrefix);

			if (commandLine.hasOption("country")) {
				orgCountry = commandLine.getOptionValue("country");
			} else {
				System.out.println("Country is not provided. Use the country option.");
				System.exit(0);
			}

			System.out.println("Org country: " + orgCountry);

			//
			//
			//
			if (commandLine.hasOption("prjFolder")) {
				projectFolder = commandLine.getOptionValue("prjFolder");
			} else {
				System.out.println("Project folder not provided. Use the prjFolder option.");
				System.exit(0);
			}

			System.out.println("Project folder: " + projectFolder);

			if (commandLine.hasOption("prjPrefix")) {
				projectPrefix = commandLine.getOptionValue("prjPrefix");
			} else {
				System.out.println("Project prefix is not provided. Use the orgPrjPrefix option.");
				System.exit(0);
			}
			System.out.println("Project prefix: " + projectPrefix);

			// options.addOption(prjFileH2020Option);
			// options.addOption(prjFileFP7Option);
			// options.addOption(orgFileH2020Option);
			// options.addOption(orgFileFP7Option);

			if (commandLine.hasOption("prjFileH2020")) {
				prjFileH2020 = commandLine.getOptionValue("prjFileH2020");
			} else {
				System.out.println("Project File H2020 is not provided. Use the prjFileH2020 option.");
				System.exit(0);
			}
			System.out.println("Project File H2020: " + prjFileH2020);

			if (commandLine.hasOption("prjFileFP7")) {
				prjFileFP7 = commandLine.getOptionValue("prjFileFP7");
			} else {
				System.out.println("Project File FP7 is not provided. Use the prjFileFP7 option.");
				System.exit(0);
			}
			System.out.println("Project File FP7: " + prjFileFP7);

			if (commandLine.hasOption("orgFileH2020")) {
				orgFileH2020 = commandLine.getOptionValue("orgFileH2020");
			} else {
				System.out.println("Organization File H2020 is not provided. Use the orgFileH2020 option.");
				System.exit(0);
			}
			System.out.println("Organization File H2020: " + orgFileH2020);

			if (commandLine.hasOption("orgFileFP7")) {
				orgFileFP7 = commandLine.getOptionValue("orgFileFP7");
			} else {
				System.out.println("Organization File FP7 is not provided. Use the orgFileFP7 option.");
				System.exit(0);
			}
			System.out.println("Organization File FP7: " + orgFileFP7);

			if (commandLine.hasOption("db")) {
				db = commandLine.getOptionValue("db");
			} else {
				System.out.println("Destination DB is not provided. Use the db option.");
				System.exit(0);
			}
			System.out.println("db: " + db);

			if (commandLine.hasOption("pubFolderOpenAire")) {
				publicationFolder = commandLine.getOptionValue("pubFolderOpenAire");
			} else {
				System.out.println("Publication folder is not provided. Use the db option.");
				System.exit(0);
			}
			System.out.println("Publication folder: " + publicationFolder);

		} catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		List<Map<String, Object>> organizations = ParserOrganization.getOrganizationsFromFilesByCountry(orgFolder,
				orgPrefix, orgCountry);

		// REMOVE only for testing
		if (DEBUG) {
			List<Map<String, Object>> organizationFilter = new ArrayList<>();
			for (Map<String, Object> org : organizations) {

				String legalname = (String) org.get("legalname");
				if (legalname.toLowerCase().equals("Institut national de la recherche agronomique".toLowerCase())) {
					organizationFilter.add(org);
				}
			}
			System.out.println("Number of filtered: " + organizationFilter.size());
			organizations = organizationFilter;
		}

		//

		System.out.println("Number of organizations: " + organizations.size());

		// ParserOrganization.visit(organizations.get(0), 0);

		if (DEBUG) {
			System.out.println("PRINT FILTERED ORGANIZATIONs");
			for (Map<String, Object> org : organizations) {
				System.out.println("==================ORGANIZATION==================");
				ParserOrganization.visit(org, 0);
			}
		}

		Set<String> projectsIds = ParserOrganization.getProjectsFromOrganizations(organizations);

		for (String projectId : projectsIds) {
			System.out.println("Project ID: " + projectId);
		}

		List<Map<String, Object>> projects = ParserProject.retrieveProjectFromOpenAireIds(projectFolder, projectPrefix,
				projectsIds);

		System.out.println("Number of " + orgCountry + " projects: " + projects.size());

		// check if project is inside

		Set<String> cordisCodesIntoProjects = new HashSet<>();

		Map<String, Map<String, Object>> projectID_projects = new HashMap<>();

		for (Map<String, Object> proj : projects) {
			String id = null;
			if (proj.containsKey("openaire_identifier_mod")) {
				id = (String) proj.get("openaire_identifier_mod");
			}
			if (id != null) {
				if (!projectID_projects.containsKey(id)) {
					projectID_projects.put(id, proj);
					String code = (String) proj.get("code");
					cordisCodesIntoProjects.add(code);
				}
			} else {
				System.err.println("Error!!! ");
			}

		}

		System.out.println("number of " + orgCountry + " projects: " + projectID_projects.size());

		//
		//
		//
		//
		//
		//
		//
		//

		List<String> pathsCompaniesCordis = new ArrayList<>();
		// pathsCompaniesCordis.add("/Users/paolosottovia/Downloads/cordis-h2020organizations_mod.csv");
		// pathsCompaniesCordis.add("/Users/paolosottovia/Downloads/cordis-fp7organizations_mod.csv");

		pathsCompaniesCordis.add(orgFileH2020);
		pathsCompaniesCordis.add(orgFileFP7);

		List<String> pathsProjectCordis = new ArrayList<>();
		// pathsProjectCordis.add("/Users/paolosottovia/Downloads/cordis-h2020projects.csv");
		// pathsProjectCordis.add("/Users/paolosottovia/Downloads/cordis-fp7projects.csv");

		pathsProjectCordis.add(prjFileH2020);
		pathsProjectCordis.add(prjFileFP7);

		Map<String, CordisOrganization> cordisOrganizations = readCordisOrganizations(pathsCompaniesCordis);
		Map<String, CordisProject> cordisProjects = readCordisProject(pathsProjectCordis);

		List<String> cordisProjectIds = new ArrayList<>();
		cordisProjectIds.addAll(cordisProjects.keySet());

		Map<String, ExternalParticipant> externalParticipants = ParserOrganization.collectExtraOrganizations(orgFolder,
				orgPrefix, projects, orgCountry, cordisOrganizations);

		System.out.println("Number of externalParticipants: " + externalParticipants.size());

		if (DEBUG) {
			for (Map.Entry<String, ExternalParticipant> entry : externalParticipants.entrySet()) {
				System.out.println(entry.getKey() + "\t" + entry.getValue().toString());
			}
		}

		System.out.println("Cordis project ID: \n");
		for (String pId : cordisProjectIds) {
			System.out.print(pId + "\t");
		}
		System.out.println("\n ====================== \n");

		// Set<String> cordisProjectIdsSet = new HashSet<>();
		// cordisProjectIdsSet.addAll(cordisProjectIds);

		System.out.println("Cordis project ID into ACTUAL PROJECTs: \n");
		for (String pId : cordisCodesIntoProjects) {
			System.out.print(pId + "\t");

		}
		System.out.println("\n ====================== \n");

		System.out.println("\nCORDIS PROJECT THAT ARE MISSING \n");
		for (String pId : cordisCodesIntoProjects) {
			if (!cordisProjects.containsKey(pId)) {
				System.out.println("ID: " + pId + "\tis missing!!!");
			}

		}
		System.out.println("\n ====================== \n");

		System.out.println("Number of cordis projects: " + cordisCodesIntoProjects.size());

		boolean TEST = false;

		System.out.println("TEST MODE: " + TEST);

		List<Map<String, Object>> openAirePublicationsRelatedToCordis = new ArrayList<>();

		if (!DEBUG) {
			openAirePublicationsRelatedToCordis = ParserPublication.getPublicationsFromFilesByProject(publicationFolder,
					"publications_", cordisCodesIntoProjects, TEST);
		}

		System.out.println("Number of publications: " + openAirePublicationsRelatedToCordis.size());

		insertCompanies(organizations, projectID_projects, cordisOrganizations, cordisProjects, db,
				openAirePublicationsRelatedToCordis, externalParticipants, DEBUG);

	}

	private static Map<String, CordisOrganization> readCordisOrganizations(List<String> paths) {

		Map<String, CordisOrganization> map = new HashMap<>();
		for (String path : paths) {

			try {
				List<CordisOrganization> beans = new CsvToBeanBuilder(new FileReader(path))
						.withType(CordisOrganization.class).withSeparator(',').build().parse();

				System.out.println("Number of beans: " + beans.size());

				for (CordisOrganization org : beans) {
					String id = org.getId();
					try {
						Integer idInt = (int) Double.parseDouble(id);
						id = idInt.toString();
					} catch (Exception e) {
						System.err.println("Error: " + id);
						System.out.println("" + org.toString());

						// System.exit(0);
					}
					if (!map.containsKey(id)) {
						map.put(id, org);
					}
				}

				System.out.println("Number of errors: ");

				System.out.println(
						"Number of organizations: " + beans.size() + " map size: " + map.size() + " path: " + path);

				// System.out.println("FP7 Number of organizations: " + beans.size());
			} catch (IllegalStateException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		List<String> keys = new ArrayList<>();
		keys.addAll(map.keySet());

		System.out.println("====================== TEST IDS ======================");
		for (int i = 0; i < keys.size(); i++) {
			if (i % 100 == 0) {
				System.out.println("\n");
			}

			System.out.print(keys.get(i) + "\t");
		}

		System.out.println("\n======================++++++++======================");

		return map;
	}

	private static Map<String, CordisProject> readCordisProject(List<String> paths) {
		Map<String, CordisProject> map = new HashMap<>();
		for (String path : paths) {
			try {
				List<CordisProject> beans = new CsvToBeanBuilder(new FileReader(path)).withType(CordisProject.class)
						.withSeparator(';').withQuoteChar('"').withSkipLines(1).build().parse();

				System.out.println("RCNs");
				for (CordisProject pr : beans) {
					System.out.print(pr.getRcn() + "\t");
				}

				// System.out.println("H2020 Number of projects: " + beans.size());
				for (CordisProject prj : beans) {
					String id = prj.getId().trim();
					if (!map.containsKey(id)) {
						map.put(id, prj);
					}
				}
			} catch (IllegalStateException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return map;
	}

	private static Map<String, List<Map<String, Object>>> groupGivenOrganizationsByOriginalId(
			List<Map<String, Object>> filesOrganizations) {
		Map<String, List<Map<String, Object>>> organizationsMap = new HashMap<>();
		for (Map<String, Object> organization : filesOrganizations) {

			// Map<String, Object> country = (Map<String, Object>)
			// organization.get("country");

			if ((organization.get("originalId")) instanceof String) {
				String id = ((String) organization.get("originalId"));
				if (id.contains("::")) {
					System.out.println("\t\tOriginalID: ");
					id = id.substring(id.indexOf("::") + 2);
				}
				System.out.println("ID: " + id);

				if (organizationsMap.containsKey(id)) {
					organizationsMap.get(id).add(organization);
				} else {
					List<Map<String, Object>> orgList = new ArrayList<>();
					orgList.add(organization);
					organizationsMap.put(id, orgList);
				}
			} else if ((organization.get("originalId") instanceof Map<?, ?>)) {
				Map<String, Object> map = (Map<String, Object>) organization.get("originalId");
				boolean stop = true;
				if (map.containsKey("originalId")) {

					if ((map.get("originalId")) instanceof String) {

						String id = ((String) map.get("originalId"));
						if (id.contains("::")) {
							System.out.println("\t\tOriginalID: ");
							id = id.substring(id.indexOf("::") + 2);
						}
						System.out.println("ID: " + id);

						if (organizationsMap.containsKey(id)) {
							organizationsMap.get(id).add(organization);
						} else {
							List<Map<String, Object>> orgList = new ArrayList<>();
							orgList.add(organization);
							organizationsMap.put(id, orgList);
						}

					} else if (map.get("originalId") instanceof Map<?, ?>) {

						Map<String, Object> map1 = (Map<String, Object>) map.get("originalId");

						if (map1.containsKey("originalId")) {

							if ((map1.get("originalId")) instanceof String) {

								String id = ((String) map1.get("originalId"));
								if (id.contains("::")) {
									System.out.println("\t\tOriginalID: ");
									id = id.substring(id.indexOf("::") + 2);
								}
								System.out.println("ID: " + id);

								if (organizationsMap.containsKey(id)) {
									organizationsMap.get(id).add(organization);
								} else {
									List<Map<String, Object>> orgList = new ArrayList<>();
									orgList.add(organization);
									organizationsMap.put(id, orgList);
								}

							}

						} else {
							System.err.println("Original ID: " + map.toString());
						}

					} else {
						System.err.println("Original ID: " + map.toString());
					}

				}

			}
		}

		return organizationsMap;
	}

	public static void insertCompanies(List<Map<String, Object>> organizations,
			Map<String, Map<String, Object>> idProject, Map<String, CordisOrganization> cordisOrganizations,
			Map<String, CordisProject> cordisProjects, String db,
			List<Map<String, Object>> openAirePublicationsRelatedToCordis,
			Map<String, ExternalParticipant> externalParticipants, boolean DEBUG) {

		if (DEBUG) {
			System.out.println("Number of organization to be inserted: " + organizations.size());
		}

		boolean debug = false;
		Map<String, List<Map<String, Object>>> codePublications = new HashMap<>();
		for (Map<String, Object> publication : openAirePublicationsRelatedToCordis) {
			if (publication.containsKey("rels")) {
				List<Map<String, Object>> rels = (List<Map<String, Object>>) publication.get("rels");

				for (Map<String, Object> rel : rels) {
					if (rel.containsKey("code")) {
						String code = (String) rel.get("code");
						System.out.println("Code: " + code);

						if (codePublications.containsKey(code)) {
							codePublications.get(code).add(publication);
						} else {
							List<Map<String, Object>> list = new ArrayList<>();
							list.add(publication);
							codePublications.put(code, list);
						}

					}
				}
			}

		}

		System.out.println("Number of projects involved into publications: " + codePublications.size());

		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);

		EntityManager entitymanager = emfactory.createEntityManager();
		// define source
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

		Source source = checkSource(sourceLabel, sourceDate, urlSource, entitymanager);
		// identifier

		// group companies

		Map<String, List<Map<String, Object>>> orginalIdCompanies = groupGivenOrganizationsByOriginalId(organizations);

		if (DEBUG) {
			System.out.println("Number of organizations: " + orginalIdCompanies.size());
		}
		// System.exit(-1);

		int n_orgs = organizations.size();
		// int n_orgs = 100;
		int i = 0;

		Map<String, Project> id_Project = new HashMap<>();
		Map<String, Publication> id_Publication = new HashMap<>();
		Map<String, Person> id_Person = new HashMap<>();

		entitymanager.getTransaction().begin();
		for (Map.Entry<String, List<Map<String, Object>>> entry : orginalIdCompanies.entrySet()) {

			if (i > n_orgs) {
				break;
			}

			if (i % 100 == 0) {
				System.out.println("Number of organizaion inserted: " + i);
			}

			List<Publication> publicationsList = new ArrayList<>();
			Set<PublicationIdentifier> publicationIdentifiers = new HashSet<>();

			List<OrganizationIdentifier> oids = new ArrayList<>();
			Organization org = new Organization();

			// Pick big entity first
			List<Map<String, Object>> orgsCollected = entry.getValue();
			if (DEBUG) {
				System.out.println("ORIGINAL ID: " + entry.getKey());
			}

			Collections.sort(orgsCollected, new Comparator<Map<String, Object>>() {

				@Override
				public int compare(Map<String, Object> o1, Map<String, Object> o2) {

					List<Map<String, Object>> rel1 = (List<Map<String, Object>>) o1.get("rels");
					List<Map<String, Object>> rel2 = (List<Map<String, Object>>) o2.get("rels");

					Integer size1 = 0;
					Integer size2 = 0;
					if (rel1 != null) {
						size1 = rel1.size();
					}
					if (rel2 != null) {
						size2 = rel2.size();
					}

					return -1 * Integer.compare(size1, size2);
				}
			});

			Map<String, Object> organizationCandidate = orgsCollected.get(0);

			// for()//

			Set<String> acronymsSet = new HashSet<>();

			for (Map<String, Object> organization : orgsCollected) {

				if (DEBUG) {
					// print orgs:
					System.out.println("\n\n\n======= ORGANIZATION =========");
					ParserOrganization.visit(organization, 0);
					System.out.println("\n\n");
				}

				String identifier = (String) organization.get("openaire_identifier");

				OrganizationIdentifier oid = new OrganizationIdentifier();
				oid.setIdentifierName("openaire_id");
				oid.setIdentifier(identifier);
				oid.setProvenance("OpenAire");
				oid.setOrganization(org);
				oid.setVisibility(false);

				if (identifier.contains("oai:dnet:")) {

					String l = identifier.replace("oai:dnet:", "");
					if (l.contains("::")) {
						String suffix = (l.substring(l.indexOf("::") + 2));

						oid.setLink("https://www.openaire.eu/search/organization?organizationId=" + "dedup_wf_001::"
								+ suffix);
					} else {

						oid.setLink("https://www.openaire.eu/search/organization?organizationId=" + l);
					}
				}

				oids.add(oid);

				String acronym = (String) organizationCandidate.get("legalshortname");

				if (acronym != null) {
					if (!acronym.trim().equals("")) {
						acronymsSet.add(acronym);
					}
				}

			}

			List<Source> sources = new ArrayList<>();
			sources.add(source);


			// add localidentifier

			// org.getOrganizationIdentifiers().add(idd);

			org.setSources(sources);

			org.setSources(sources);
			// String acronym = (String) organizationCandidate.get("legalshortname");

			org.setLabel(((String) organizationCandidate.get("legalname")).replace("&quot;", "\""));

			String urlCordis = null;
			if (cordisOrganizations.containsKey(entry.getKey())) {
				if (DEBUG) {
					System.out.println("ENTITY  FOUND IN CORDIS");
				}

				OrganizationIdentifier oid = new OrganizationIdentifier();
				oid.setIdentifierName("PID");
				oid.setIdentifier(entry.getKey());
				oid.setProvenance("OpenAire");
				oid.setOrganization(org);
				oids.add(oid);

				CordisOrganization cordisOrg = cordisOrganizations.get(entry.getKey());
				String city = cordisOrg.getCity();
				String postCode = cordisOrg.getPostCode();
				String street = cordisOrg.getStreet();

				org.setAddress(street);
				org.setPostcode(postCode);
				org.setCity(city);

				if (cordisOrg.getShortName() != null) {
					if (!cordisOrg.getShortName().equals("")) {
						acronymsSet.add(cordisOrg.getShortName());
					}
				}

				urlCordis = cordisOrg.getOrganizationUrl();

			} else {
				if (DEBUG) {
					System.out.println("ENTITY NOT FOUND IN CORDIS");
				}
			}

			List<Link> links = new ArrayList<>();

			for (Map<String, Object> organization : orgsCollected) {
				String url = (String) organization.get("websiteurl");

				if (url != null) {
					if (!url.trim().equals("")) {
						// org.set
						Link l = new Link();
						l.setUrl(url);
						l.setLabel("HomePage");
						l.setType("URL");
						l.setSources(sources);
						links.add(l);

					} else {
						if (urlCordis != null) {
							if (!urlCordis.trim().equals("")) {
								Link l = new Link();
								l.setUrl(urlCordis);
								l.setLabel("HomePage");
								l.setType("URL");
								l.setSources(sources);
								links.add(l);
							}
						}
					}
				} else {
					if (urlCordis != null) {
						if (!urlCordis.trim().equals("")) {
							Link l = new Link();
							l.setUrl(urlCordis);
							l.setLabel("HomePage");
							l.setType("URL");
							l.setSources(sources);
							links.add(l);
						}
					}
				}
			}
			org.setLinks(links);

			String country = (String) ((Map<String, Object>) organizationCandidate.get("country")).get("classname");
			String countryCode = (String) ((Map<String, Object>) organizationCandidate.get("country")).get("classid");

			org.setCountry(country);
			org.setCountryCode(countryCode);

			org.setAddressSources(sources);

			org.setIsPublic("undefined");

			List<String> acronyms = new ArrayList<>();
			acronyms.addAll(acronymsSet);

			org.setAcronyms(acronyms);
			
			
			
			
			// organization identifiers
			org.setOrganizationIdentifiers(oids);
			String identifierLocal = LocalIdentifierGenerator.getOrgIds(org);

			OrganizationIdentifier idd = new OrganizationIdentifier();
			idd.setIdentifier(identifierLocal);
			idd.setIdentifierName("lid");
			idd.setOrganization(org);
			idd.setProvenance("OpenAire");
			idd.setVisibility(false);

			oids.add(idd);

			org.setOrganizationIdentifiers(oids);
			


			// extra fields
			populateExtraFields(org, organizationCandidate);

			// add projects connections....

			List<Project> projectList = new ArrayList<>();
			for (Map<String, Object> organization : orgsCollected) {
				List<Map<String, Object>> projects = (List<Map<String, Object>>) organization.get("rels");

				System.out.println("Number of projects: " + projects);

				for (Map<String, Object> proj : projects) {
					Project PROJECT;
					String prjID = (String) ((Map<String, Object>) proj.get("to")).get("value");

					if (DEBUG) {
						System.out.println("\n\n\n=================PROJECT=======================");

						ParserOrganization.visit(proj, 0);

						System.out.println("===============================================\n\n\n");
					}

					System.out.println("PROJECT ID: " + prjID);

					Map<String, Object> projOpenAire = idProject.get(prjID);

					if (projOpenAire == null) {
						System.err.println("ERROR: empty project ID: " + prjID);
						System.err.println(proj.toString());
					} else {

						if (DEBUG) {
							System.out.println("\n\n\n=================PROJECT=======================");
							System.out.println("PROJECT OPENAIRE: ");
							ParserOrganization.visit(projOpenAire, 0);

							System.out.println("===============================================\n\n\n");
						}

						// check if id is inside

						if (id_Project.containsKey(prjID)) {

							System.out.println("Project already inserted" + "!!!");

							PROJECT = id_Project.get(prjID);
							projectList.add(PROJECT);

						} else {
							// otherwise insert it

							PROJECT = new Project();
							// PROJECT.set

							String code = ((String) proj.get("code")).trim();

							if (DEBUG) {
								System.out.println("CODE: " + code);
							}

							String startDate = null;
							if (projOpenAire.containsKey("startdate")) {
								startDate = (String) projOpenAire.get("startdate");

							}

							String endDate = null;
							if (projOpenAire.containsKey("enddate")) {
								endDate = (String) projOpenAire.get("enddate");
							}

							String duration = (String) projOpenAire.get("duration");

							PROJECT.setDuration(duration);

							// System.out.println("Start date: " + startDate);
							if (startDate != null) {
								if (!startDate.trim().equals("")) {
									try {
										PROJECT.setStartDate(df.parse(startDate));

										String START_DATE_STRING = df.format(PROJECT.getStartDate());
										if (START_DATE_STRING.contains("-")) {
											String[] el = startDate.split("-");
											if (el.length == 3) {
												PROJECT.setYear(el[0]);
												PROJECT.setMonth(el[1]);
											}

										}

										Date end_Date = df.parse(endDate);
										Date start_Date = df.parse(startDate);

										int durationCount = monthsBetweenDates(start_Date, end_Date);

										if (PROJECT.getDuration() == null) {
											PROJECT.setDuration(durationCount + "");
										}

										//
										//

									} catch (ParseException e) {
										e.printStackTrace();
									}
								}
							}

							// PROJECT.(new Date(Integer.parseInt(endDate.split("-")[0]),
							// Integer.parseInt(endDate.split("-")[1]),
							// Integer.parseInt(endDate.split("-")[2])));

							String websiteurl = (String) projOpenAire.get("websiteurl");

							PROJECT.setUrl(websiteurl);
							List<ProjectIdentifier> projectIdentifiers = new ArrayList<>();
							String openaire_identifier = (String) projOpenAire.get("openaire_identifier_mod");

							if (cordisProjects.containsKey(code)) {

								if (DEBUG) {
									System.out.println("PROJECT in CORDIS");

								}

								List<ExternalParticipant> externalParticipantsList = new ArrayList<>();

								if (projOpenAire.containsKey("rels")) {
									List<Map<String, Object>> rels = (List<Map<String, Object>>) projOpenAire
											.get("rels");

									for (Map<String, Object> rel : rels) {

										if (rel.containsKey("to")) {
											Map<String, Object> to = (Map<String, Object>) rel.get("to");
											if (to.containsKey("clazz")) {
												String clazz = (String) to.get("clazz");

												if (clazz.equals("hasParticipant")) {
													String value = (String) to.get("value");

													if (externalParticipants.containsKey(value)) {
														externalParticipantsList.add(externalParticipants.get(value));
													}

												}

											}

										}

									}

								}

								PROJECT.setExternalParticipants(externalParticipantsList);

								if (DEBUG) {
									System.out.println("EXTERNAL PARITICIPANTS:");
									for (ExternalParticipant ext : externalParticipantsList) {
										System.out.println("\t" + ext.toString());
									}
								}

								CordisProject prj = cordisProjects.get(code);
								String cordis_acr = prj.getAcronym();

								String cordis_url = prj.getProjectUrl();
								// FIX HERE
								String cordis_call = prj.getCall();
								String cordis_call_id = prj.getCall();
								if (PROJECT.getCallId() == null) {
									PROJECT.setCallId(cordis_call);
								} else {
									if (PROJECT.getCallId().trim().equals("")) {
										PROJECT.setCallId(cordis_call);
									}
								}
								String cordis_coordinator = prj.getCoordinator();
								String cordis_coordinatorCountry = prj.getCoordinatorCountry();
								String cordis_ecMaxContribution = prj.getEcMaxContribution();

								String cordis_endDate = prj.getEndDate();
								String cordis_frameworkProgramme = prj.getFrameworkProgramme();
								String cordis_fundingScheme = prj.getFundingScheme();
								String cordis_id = prj.getId();
								String cordis_objective = prj.getObjective();
								if (cordis_objective != null) {
									cordis_objective = cordis_objective.replace("&quot;", "\"");
								}

								if (PROJECT.getDescription() == null) {
									PROJECT.setDescription(cordis_objective);
								} else {
									if (PROJECT.getDescription().trim().equals("")) {
										PROJECT.setDescription(cordis_objective);
									}
								}

								Date end_Date;
								try {
									end_Date = df.parse(prj.getEndDate());
									Date start_Date = df.parse(prj.getStartDate());
									int durationCount = monthsBetweenDates(start_Date, end_Date);

									if (PROJECT.getDuration() == null) {
										PROJECT.setDuration(durationCount + "");
									}
								} catch (ParseException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}

								String cordis_participantCountries = prj.getParticipantCountries();
								String cordis_participants = prj.getParticipants();
								String cordis_programme = prj.getProgramme();
								PROJECT.setType(cordis_frameworkProgramme);

								// String cordis_url=prj.getProjectUrl();
								String cordis_rcn = prj.getRcn();
								String cordis_startDate = prj.getStartDate();
								String cordis_status = prj.getStatus();
								String cordis_subjects = prj.getSubjects();
								String cordis_title = prj.getTitle();
								String cordis_topics = prj.getTopics();
								String cordis_total_cost = prj.getTotalCost().replace(",", ".");
								if (PROJECT.getBudget() == null) {
									PROJECT.setBudget(cordis_total_cost);
								} else {
									if (PROJECT.getBudget().trim().equals("")) {
										PROJECT.setBudget(cordis_total_cost);
									}
								}

								if (cordis_url != null) {
									if (!cordis_url.trim().equals("")) {
										if (PROJECT.getUrl() == null) {
											PROJECT.setUrl(cordis_url);
										} else if (PROJECT.getUrl().trim().equals("")) {
											PROJECT.setUrl(cordis_url);
										}
									}
								}

								// PROJECT.get

								PROJECT.setUrl("http://cordis.europa.eu/project/rcn/" + prj.getRcn() + "_en.html");

								ProjectExtraField pEx1 = new ProjectExtraField();
								pEx1.setFieldKey("contractNumber");
								pEx1.setFieldValue(prj.getId() + "");
								pEx1.setProject(PROJECT);
								pEx1.setVisibility(true);
								List<ProjectExtraField> extraProjectFields = new ArrayList<>();
								extraProjectFields.add(pEx1);

								PROJECT.setProjectExtraFields(extraProjectFields);

								if (DEBUG) {

									System.out.println("getAcronym\t" + prj.getAcronym());
									System.out.println("getCall\t" + prj.getCall());
									System.out.println("getCoordinator\t" + prj.getCoordinator());
									System.out.println("getCoordinatorCountry\t" + prj.getCoordinatorCountry());
									System.out.println("getEcMaxContribution\t" + prj.getEcMaxContribution());
									System.out.println("getEndDate\t" + prj.getEndDate());
									System.out.println("getFrameworkProgramme\t" + prj.getFrameworkProgramme());
									System.out.println("getFundingScheme\t" + prj.getFundingScheme());
									System.out.println("getId\t" + prj.getId());
									System.out.println("getObjective\t" + prj.getObjective());
									System.out.println("getParticipantCountries\t" + prj.getParticipantCountries());
									System.out.println("getParticipants\t" + prj.getParticipants());
									System.out.println("getProgramme\t" + prj.getProgramme());
									System.out.println("getProjectUrl\t" + prj.getProjectUrl());
									System.out.println("getRcn\t" + prj.getRcn());
									System.out.println("getStartDate\t" + prj.getStartDate());
									System.out.println("getStatus\t" + prj.getStatus());
									System.out.println("getSubjects\t" + prj.getSubjects());
									System.out.println("getSubjects\t" + prj.getSubjects());
									System.out.println("getTitle\t" + prj.getTitle());
									System.out.println("getTopics\t" + prj.getTopics());
									System.out.println("getTotalCost\t" + prj.getTotalCost());

									System.out.println("\n\n\n");

								}

							} else {
								if (DEBUG) {
									System.out.println("Project is not prosent in CORDIS");
								}
							}

							// System.out.println("OpenAireIdentifier: " + openaire_identifier);

							ProjectIdentifier iden = new ProjectIdentifier();
							iden.setIdentifier(openaire_identifier);
							iden.setProvenance("OpenAire");
							iden.setIdentifierName("openaire_id");
							iden.setProject(PROJECT);
							projectIdentifiers.add(iden);
							PROJECT.setProjectIdentifiers(projectIdentifiers);

							if (projOpenAire.containsKey("title")) {
								if (debug) {
									System.out.println(projOpenAire.get("title").toString());
								}
								String title = (String) (projOpenAire.get("title"));
								if (title != null) {
									title = title.replace("&quot;", "\"");
								}
								PROJECT.setLabel(title);
							} else {
								System.err.println("EMPTY TITLE!");
							}
							if (projOpenAire.containsKey("acronym")) {
								if (debug) {
									System.out.println(projOpenAire.get("acronym"));
								}
								String acronymPrj = (String) projOpenAire.get("acronym");
								PROJECT.setAcronym(acronymPrj);
							} else {
								System.err.println("EMPTY ACR!");
							}

							PROJECT.setSources(sources);

							List<Publication> pubs = insertPublicationIntoProjects(codePublications.get(code), sources,
									entitymanager, PROJECT, id_Publication, id_Person);

							for (Publication pub : pubs) {
								pub.setSources(sources);

								for (PublicationIdentifier identifi : pub.getPublicationIdentifiers()) {
									if (publicationIdentifiers.contains(identifi)) {

									} else {
										publicationIdentifiers.add(identifi);
										publicationsList.add(pub);
									}
								}
							}
							//

							// publicationsList.addAll(pubs);

							entitymanager.persist(PROJECT);
							id_Project.put(prjID, PROJECT);
							projectList.add(PROJECT);

						}
					}

				}

			}

			org.setProjects(projectList);

			org.setPublications(publicationsList);
			// retrieve projects and connect them to the db;

			entitymanager.persist(org);

			i++;

		}

		System.out.println("BEFORE COMMIT!");
		entitymanager.getTransaction().commit();

		entitymanager.close();
		emfactory.close();

		System.out.println("End of the insertion process!!!");

		System.out.println("Number of project: " + id_Project.size());
		System.out.println("Number of publication: " + id_Publication.size());
		System.out.println("Number of people: " + id_Person.size());

		System.out.println("Number of companies: " + i);

	}

	private static int monthsBetweenDates(Date startDate, Date endDate) {

		Calendar start = Calendar.getInstance();
		start.setTime(startDate);

		Calendar end = Calendar.getInstance();
		end.setTime(endDate);

		int monthsBetween = 0;
		int dateDiff = end.get(Calendar.DAY_OF_MONTH) - start.get(Calendar.DAY_OF_MONTH);

		if (dateDiff < 0) {
			int borrrow = end.getActualMaximum(Calendar.DAY_OF_MONTH);
			dateDiff = (end.get(Calendar.DAY_OF_MONTH) + borrrow) - start.get(Calendar.DAY_OF_MONTH);
			monthsBetween--;

			if (dateDiff > 0) {
				monthsBetween++;
			}
		} else {
			monthsBetween++;
		}
		monthsBetween += end.get(Calendar.MONTH) - start.get(Calendar.MONTH);
		monthsBetween += (end.get(Calendar.YEAR) - start.get(Calendar.YEAR)) * 12;
		return monthsBetween;
	}

	private static List<Publication> insertPublicationIntoProjects(List<Map<String, Object>> publications,
			List<Source> sources, EntityManager entitymanager, Project project, Map<String, Publication> id_Publication,
			Map<String, Person> id_Person) {

		List<Publication> listPublications = new ArrayList<>();

		if (publications == null) {
			return listPublications;
		}

		for (Map<String, Object> pub : publications) {
			// Publication pub = new Publication();

			if (pub == null) {
				continue;
			}

			List<PublicationIdentifier> publicationIdentifiers = new ArrayList<>();
			PublicationIdentifier publicationIdentifier = new PublicationIdentifier();
			publicationIdentifier.setProvenance("OpenAire");

			// String openaire_identifier = (String) publication.get("openaire_identifier");

			String openaire_identifier = (String) pub.get("openaire_identifier");
			System.out.println("openaire identifier: " + openaire_identifier);
			publicationIdentifier.setIdentifier(openaire_identifier);
			publicationIdentifier.setIdentifierName("openaire_identifier");
			// publicationIdentifiers.add(publicationIdentifier);

			// check for publicaiton id

			// Publication publicat = checkPublication(openaire_identifier, entitymanager);
			Publication publicat = null;
			if (id_Publication.containsKey(openaire_identifier)) {
				publicat = id_Publication.get(openaire_identifier);
			}

			if (publicat != null) {
				if (!publicat.getProjects().contains(project)) {
					publicat.getProjects().add(project);

				}
				listPublications.add(publicat);
				continue;
			}

			// String
			publicat = new Publication();
			List<Project> projs = new ArrayList<>();
			projs.add(project);
			publicat.setProjects(projs);
			String dateofacceptance = (String) ((Map<String, Object>) pub.get("dateofacceptance")).get("value");

			String description = null;

			if (pub.get("description") instanceof String) {
				description = (String) pub.get("description");
			} else if (pub.get("description") instanceof Map<?, ?>) {
				Map<String, Object> desc = (Map<String, Object>) pub.get("description");
				System.err.println("DESCRIPTION MAP: " + desc.toString());

			}
			String title = (String) ((Map<String, Object>) pub.get("title")).get("content");

			if (pub.containsKey("journal")) {
				String location_name = (String) ((Map<String, Object>) pub.get("journal")).get("value");
				publicat.setLocationName(location_name);
				publicat.setLocationType("journal");
			}
			if (description != null) {
				description = description.replace("&quot;", "\"");
			}
			publicat.setDescription(description);

			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

			Date publicationDate = null;
			try {
				publicationDate = df.parse(dateofacceptance);
				// source.setRevisionDate(sourceDate);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// if (true) {
			// throw new RuntimeException("FIX");
			// }
			// publicat.setPublicationDate(publicationDate);

			// http://dx.doi.org/10.1038%2Fonc.2014.319

			String url = null;

			if (pub.containsKey("pid")) {
				String a = (String) ((Map<String, Object>) pub.get("pid")).get("content");
				if (a != null) {
					if (!a.trim().equals("")) {
						url = "http://dx.doi.org/" + a;
					}
				}
			}
			publicat.setUrl(url);

			for (Map<String, Object> rel : (List<Map<String, Object>>) pub.get("rels")) {

				List<Person> people = new ArrayList<>();

				System.out.println(rel.toString() + "\n");
				if (rel.containsKey("ranking")) {
					Map<String, Object> to = (Map<String, Object>) rel.get("to");
					if (to.containsKey("clazz")) {
						String clazz = (String) to.get("clazz");
						if (clazz.equals("hasAuthor")) {

							String personIdentifier = (String) to.get("value");
							PersonIdentifier pId = new PersonIdentifier();
							pId.setIdentifier(personIdentifier);
							pId.setIdentifierName("openaire_identifier");
							pId.setProvenance("OpenAire");

							List<PersonIdentifier> personIdenfiers = new ArrayList<>();
							personIdenfiers.add(pId);

							String personFullName = (String) rel.get("fullname");

							System.out.println(personIdentifier + "\t" + personFullName);

							// Person pers = checkPerson(personIdentifier, entitymanager);

							Person pers = null;
							if (id_Person.containsKey(personIdentifier)) {
								pers = id_Person.get(personIdentifier);
							}

							if (pers != null) {
								people.add(pers);
								continue;
							}

							Person person = new Person();
							person.setSources(sources);
							boolean error = false;
							if (personFullName.contains(",")) {
								String[] elName = personFullName.split(",");
								if (elName.length == 2) {
									person.setFirstName(elName[1]);
									person.setLastName(elName[0]);
								} else {
									error = true;
								}
							} else if (personFullName.contains(" ")) {
								String[] elName = personFullName.split(" ");
								if (elName.length == 2) {
									person.setFirstName(elName[elName.length - 1]);
									person.setLastName(elName[0]);
								} else {
									error = true;
								}
							} else {
								System.err.println("STRANGE NAME!!! " + personFullName);
							}

							for (PersonIdentifier pid : personIdenfiers) {
								pid.setPerson(person);
							}

							person.setPersonIdentifiers(personIdenfiers);
							person.setSources(sources);
							if (!error) {
								people.add(person);
								entitymanager.persist(person);
								id_Person.put(personIdentifier, person);
							}

						}
					}

				}
				publicat.setAuthors(people);
				entitymanager.persist(publicat);
				id_Publication.put(openaire_identifier, publicat);
				listPublications.add(publicat);

			}

		}
		return listPublications;
	}

	private static void convertRawDataToPublications(List<Map<String, Object>> publications) {

	}

	private static void populateExtraFields(Organization org, Map<String, Object> organizationMap) {
		List<OrganizationExtraField> extrafields = new ArrayList<>();
		OrganizationExtraField field1 = new OrganizationExtraField();
		field1.setFieldKey("ecnonprofit");
		field1.setFieldValue((String) organizationMap.get("ecnonprofit"));
		field1.setVisibility(true);
		field1.setOrganization(org);
		OrganizationExtraField field2 = new OrganizationExtraField();
		field2.setFieldKey("eclegalbody");
		field2.setFieldValue((String) organizationMap.get("eclegalbody"));
		field2.setVisibility(true);
		field2.setOrganization(org);
		OrganizationExtraField field3 = new OrganizationExtraField();
		field3.setFieldKey("eclegalperson");
		field3.setFieldValue((String) organizationMap.get("eclegalperson"));
		field3.setVisibility(true);
		field3.setOrganization(org);
		OrganizationExtraField field4 = new OrganizationExtraField();
		field4.setFieldKey("ecenterprise");
		field4.setFieldValue((String) organizationMap.get("ecenterprise"));
		field4.setVisibility(true);
		field4.setOrganization(org);
		OrganizationExtraField field5 = new OrganizationExtraField();
		field5.setFieldKey("ecnutscode");
		field5.setFieldValue((String) organizationMap.get("ecnutscode"));
		field5.setVisibility(true);
		field5.setOrganization(org);
		OrganizationExtraField field6 = new OrganizationExtraField();
		field6.setFieldKey("ecresearchorganization");
		field6.setFieldValue((String) organizationMap.get("ecresearchorganization"));
		field6.setVisibility(true);
		field6.setOrganization(org);
		OrganizationExtraField field7 = new OrganizationExtraField();
		field7.setFieldKey("ecinternationalorganization");
		field7.setFieldValue((String) organizationMap.get("ecinternationalorganization"));
		field7.setVisibility(true);
		field7.setOrganization(org);
		OrganizationExtraField field8 = new OrganizationExtraField();
		field8.setFieldKey("ecinternationalorganizationeurinterests");
		field8.setFieldValue((String) organizationMap.get("ecinternationalorganizationeurinterests"));
		field8.setVisibility(true);
		field8.setOrganization(org);
		OrganizationExtraField field9 = new OrganizationExtraField();
		field9.setFieldKey("echighereducation");
		field9.setFieldValue((String) organizationMap.get("echighereducation"));
		field9.setVisibility(true);
		field9.setOrganization(org);

		extrafields.add(field1);
		extrafields.add(field2);
		extrafields.add(field3);
		extrafields.add(field4);
		extrafields.add(field5);
		extrafields.add(field6);
		extrafields.add(field7);
		extrafields.add(field8);
		extrafields.add(field9);

		org.setOrganizationExtraFields(extrafields);

	}

	private static int checkProject(String id, EntityManager entitymanager) {
		boolean debug = false;
		if (debug) {
			System.out.println("Check id: " + id);
		}
		Query query = entitymanager.createQuery("SELECT p FROM ProjectIdentifier p where p.identifier= :id ");
		query.setParameter("id", id);

		List<ProjectIdentifier> results = query.getResultList();

		ProjectIdentifier p = null;
		// System.out.println("******************");
		for (ProjectIdentifier proj : results) {
			if (debug) {
				System.out.println(proj.getIdentifier());
			}
		}
		// System.out.println("******************");
		if (results.size() > 0) {
			if (results.get(0) != null) {
				p = results.get(0);
				if (debug) {
					System.out.println("ESISTE Progetto");
				}
				return p.getId();
			}
		} else {
			// orgType = new OrganizationType();
			// orgType.setLabel(type);
			if (debug) {
				System.out.println("NON ESISTE PROGETTO: add new Project -1 ");
			}

		}

		return -1;
	}

	private static Person checkPerson(String id, EntityManager entitymanager) {
		boolean debug = false;

		PersonIdentifier identifier = new PersonIdentifier();

		Query query = entitymanager
				.createQuery("Select i FROM PersonIdentifier i WHERE i.identifier_value = :identifier");
		query.setParameter("identifier", id);

		List<PersonIdentifier> results = query.getResultList();
		// System.out.println("******************");
		for (PersonIdentifier iden : results) {
			if (debug) {
				System.out.println(iden.getIdentifier());
			}
		}
		// System.out.println("******************");
		if (results.size() > 0) {
			if (results.get(0) != null) {
				identifier = results.get(0);
				if (debug) {
					System.out.println("ESISTE PersonIdentifier");
				}

				Query query1 = entitymanager
						.createQuery("Select i FROM Person i WHERE :id  MEMBER OF p.personIdentifiers");
				query1.setParameter("id", identifier);
				List<Person> results1 = query1.getResultList();

				return results1.get(0);
			}
		} else {
			// orgType = new OrganizationType();
			// orgType.setLabel(type);
			if (debug) {
				System.out.println("NON ESISTE Publication");
			}
			return null;
		}

		return null;

	}

	private static Publication checkPublication(String id, EntityManager entitymanager) {
		boolean debug = false;

		PublicationIdentifier identifier = new PublicationIdentifier();

		Query query = entitymanager
				.createQuery("Select i FROM PublicationIdentifier i WHERE i.identifier = :identifier");
		query.setParameter("identifier", id);

		List<PublicationIdentifier> results = query.getResultList();
		// System.out.println("******************");
		for (PublicationIdentifier iden : results) {
			if (debug) {
				System.out.println(iden.getIdentifier());
			}
		}
		// System.out.println("******************");
		if (results.size() > 0) {
			if (results.get(0) != null) {
				identifier = results.get(0);
				if (debug) {
					System.out.println("ESISTE Publication");
				}

				Query query1 = entitymanager
						.createQuery("Select i FROM Publication i WHERE :id  MEMBER OF p.publicationIdentifiers");
				query1.setParameter("id", identifier);
				List<Publication> results1 = query1.getResultList();

				return results1.get(0);
			}
		} else {
			// orgType = new OrganizationType();
			// orgType.setLabel(type);
			if (debug) {
				System.out.println("NON ESISTE Publication");
			}
			return null;
		}

		return null;

	}

	public static Source checkSource(String sourceLabel, Date revisionDate, String url, EntityManager entitymanager) {
		boolean debug = false;
		Query query = entitymanager
				.createQuery("SELECT s FROM Source s WHERE s.label = :label and s.revisionDate = :revision_date");
		query.setParameter("label", sourceLabel);
		query.setParameter("revision_date", revisionDate);

		Source s = new Source();
		s.setLabel(sourceLabel);
		s.setRevisionDate(revisionDate);
		s.setUrl(url);

		List<Source> results = query.getResultList();
		// System.out.println("******************");
		for (Source source : results) {
			if (debug) {
				System.out.println(source.getLabel());
			}
		}
		// System.out.println("******************");
		if (results.size() > 0) {
			if (results.get(0) != null) {
				s = results.get(0);
				// System.out.println("ESISTE SOURCE");
			}
		} else {
			// orgType = new OrganizationType();
			// orgType.setLabel(type);
			// System.out.println("NON ESISTE SOURCE: add new source");

		}

		// date = new Date(year, month, date)

		// s.setRevisionDate(revisionDate);

		return s;
	}

	private static boolean checkExistanceOfOrganization(Map<String, Object> organization, EntityManager entitymanager,
			String provenance) {
		boolean debug = false;
		String id = (String) organization.get("openaire_identifier");

		Organization o;
		// o.get

		OrganizationIdentifier identifier = new OrganizationIdentifier();

		// identifier.setProvenance("OpenAire");

		// OrganizationIdentifier

		Query query = entitymanager.createQuery(
				"Select i FROM OrganizationIdentifier i WHERE i.identifier = :identifier and i.provenance = :provenance");
		query.setParameter("identifier", id);
		query.setParameter("provenance", provenance);

		List<OrganizationIdentifier> results = query.getResultList();
		// System.out.println("******************");
		for (OrganizationIdentifier iden : results) {
			if (debug) {
				System.out.println(iden.getIdentifier());
			}
		}
		// System.out.println("******************");
		if (results.size() > 0) {
			if (results.get(0) != null) {
				identifier = results.get(0);
				if (debug) {
					System.out.println("ESISTE");
				}
				return true;
			}
		} else {
			// orgType = new OrganizationType();
			// orgType.setLabel(type);
			if (debug) {
				System.out.println("NON ESISTE");
			}
			return false;
		}

		return false;

	}

}
