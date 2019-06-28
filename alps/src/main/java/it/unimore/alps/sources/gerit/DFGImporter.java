package it.unimore.alps.sources.gerit;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import org.json.JSONArray;
import org.json.JSONObject;

import it.unimore.alps.sql.model.Link;
import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationIdentifier;
import it.unimore.alps.sql.model.Project;
import it.unimore.alps.sql.model.Source;

public class DFGImporter {

	public static Integer countIdentifiers = 0;

	public static void main(String[] args) {

		CommandLine commandLine;
		Option orgFolderOption = Option.builder("orgFolder").hasArg().required(true)
				.desc("The file that contains organization data. ").longOpt("organizationFolder").build();

		Option prjFolderOption = Option.builder("prjFolder").hasArg().required(true)
				.desc("The file that contains organization data. ").longOpt("projectFolder").build();
		//
		Option orgExcelFileOption = Option.builder("orgExcelFile").hasArg().required(true)
				.desc("The prefix that identify files  that contains organization data. ")
				.longOpt("organizationExcelFile").build();

		Option dbOption = Option.builder("db").hasArg().required(true)
				.desc("The name of the database of the data ingestion.").longOpt("DB").build();

		Options options = new Options();

		options.addOption(orgFolderOption);
		options.addOption(orgExcelFileOption);
		options.addOption(dbOption);
		options.addOption(prjFolderOption);

		CommandLineParser parser = new DefaultParser();

		String orgFolder = null;
		String orgExcelFile = null;

		String orgProjectFile = null;

		String prjFolder = null;

		String db = null;

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

			if (commandLine.hasOption("prjFolder")) {
				prjFolder = commandLine.getOptionValue("prjFolder");
			} else {
				System.out.println("Project folder not provided. Use the orgFolder option.");
				System.exit(0);
			}
			System.out.println("Project folder: " + prjFolder);

			if (commandLine.hasOption("orgExcelFile")) {
				orgExcelFile = commandLine.getOptionValue("orgExcelFile");
			} else {
				System.out.println("Organization folder not provided. Use the orgExcelFile option.");
				System.exit(0);
			}
			System.out.println("Org ExcelFile: " + orgExcelFile);

			if (commandLine.hasOption("db")) {
				db = commandLine.getOptionValue("db");
			} else {
				System.out.println("Destination DB is not provided. Use the db option.");
				System.exit(0);
			}
			System.out.println("db: " + db);

		} catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// String folderPath =
		// "/Users/paolosottovia/eclipse-workspace/crawler/jsonData/";

		ParserDFG p = new ParserDFG();

		String fileName = "/Users/paolosottovia/Downloads/institutionen_gerit___.xlsx";

		List<DFGOrganization> orgs = p.readInitialFile(orgExcelFile);

		boolean test = true;

		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);

		EntityManager entitymanager = emfactory.createEntityManager();
		// define source
		String sourceRevisionDate = "2019-03-06";
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

		entitymanager.getTransaction().begin();

		Date sourceDate = null;
		try {
			sourceDate = df.parse(sourceRevisionDate);
			// source.setRevisionDate(sourceDate);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Date revisionDate = new Date(2017, 12, 15);
		String sourceLabel = "Gerit";
		String urlSource = "http://www.gerit.org";

		Source source = checkSource(sourceLabel, sourceDate, urlSource, entitymanager);

		// entitymanager.persist(source);

		// Just for testing
		// Source source = new Source();

		List<Source> sources = new ArrayList<>();
		sources.add(source);

		// add localidentifier

		// org.getOrganizationIdentifiers().add(idd);

		DFGImporter importer = new DFGImporter();

		Map<Integer, Project> idProject = new HashMap<>();
		Map<Integer, List<Integer>> orgID_projects = new HashMap<>();

		importer.loadProjects(prjFolder, idProject, orgID_projects, sources);

		System.out.println("Number of projects: " + idProject.size());
		int relationCount = 0;
		for (List<Integer> prjs : orgID_projects.values()) {
			relationCount += prjs.size();
		}

		System.out.println("Number of project relationships: " + relationCount);

//		System.exit(0);

		Map<Integer, List<Integer>> fatherChildren = new HashMap<>();
		Set<Integer> mainOrganizations = new HashSet<>();
		for (DFGOrganization o : orgs) {
			mainOrganizations.add(o.getId());
		}

		Map<Integer, Organization> idOrgs = importer.retrieveAllOrganizations(orgFolder, sources, fatherChildren,
				mainOrganizations);

		System.out.println("Number of keys: " + idOrgs.size());

//		System.out.println("Number of fathers: " + fatherChildren.keySet().size());
//
//		System.out.println("ID: " + 10284 + "  " + fatherChildren.get(10284).toString());
//
//		System.out.println("ID: " + 13344 + "  " + fatherChildren.get(13344).toString());

		Set<Integer> ids = new HashSet<>();
		ids.addAll(idOrgs.keySet());
		for (Integer id : ids) {

			if (orgID_projects.containsKey(id)) {
				List<Integer> projectIds = orgID_projects.get(id);

				List<Project> projects = new ArrayList<>();
				for (Integer i : projectIds) {
					projects.add(idProject.get(i));
				}

				idOrgs.get(id).setProjects(projects);
			}

			entitymanager.persist(idOrgs.get(id));

		}

		List<Integer> fathers = new ArrayList<>();
		fathers.addAll(fatherChildren.keySet());

		for (Integer father : fathers) {

			if (fatherChildren.containsKey(father)) {
				List<Organization> childs = new ArrayList<>();
				for (Integer child : fatherChildren.get(father)) {
					childs.add(idOrgs.get(child));
				}

				if (!idOrgs.containsKey(father)) {
					System.err.println("ERROR!: missing item: " + father);
				} else {

					idOrgs.get(father).setChildrenOrganizations(childs);
					entitymanager.merge(idOrgs.get(father));
				}
			}
		}

		System.out.println("BEFORE COMMIT!");
		entitymanager.getTransaction().commit();

		entitymanager.close();
		emfactory.close();

		System.out.println("END of the process!!!");
		System.out.println("Number of identifiers: " + countIdentifiers);

	}

	public void loadProjects(String folder, Map<Integer, Project> idProject, Map<Integer, List<Integer>> orgID_projects,
			List<Source> sources) {
		File f = new File(folder);
		boolean debug = true;
		for (File file : f.listFiles()) {

			String filename = file.getName();
			if (filename.contains(".json")) {

				String id_ = filename.replace(".json", "");
				if (debug) {
					System.out.println("Organization id: " + id_);
				}
				Integer organizationID = Integer.parseInt(id_);

				List<Project> projects = readProjects(file, sources);
				List<Integer> ids = new ArrayList<>();
				for (Project p : projects) {

					Integer id = Integer.parseInt(p.getCallId());
					if (ids.contains(id)) {
						System.err.println("DUPLICATE PROJECT: " + id + " organization: " + organizationID);
					} else {
						ids.add(id);
					}
					idProject.put(id, p);
					if (debug) {
						System.out.println("\tproject_id: " + id);
					}
				}

				orgID_projects.put(organizationID, ids);

			}

		}
	}

	private List<Project> readProjects(File file, List<Source> sources) {

		List<Project> projects = new ArrayList<>();

		try {

			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF8"));

			String str;

			while ((str = in.readLine()) != null) {

				if (!str.trim().equals("")) {

					DFGProject p = DFGProject.fromJsonToDFGProject(str);

					Project project = translateProject(p, sources);
					projects.add(project);
				}

			}

			in.close();
		} catch (UnsupportedEncodingException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println(e.getMessage());
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}

		return projects;
	}

	private Project translateProject(DFGProject prj, List<Source> sources) {

		Project project = new Project();
		project.setCallId(prj.getId());
		project.setCallLabel(prj.getProgramme());
		project.setDescription(prj.getDescription());
		project.setDuration(project.getDuration());
		// project.setStartDate(prj.getStartDate());

		project.setLabel(prj.getName());
		project.setUrl(prj.getUrl());
		project.setSources(sources);

		return project;
	}

	public void extractHierarchicalStructure(JSONObject tree, Map<Integer, List<Integer>> fatherChildren) {

		Integer id = tree.getInt("id");
//		System.out.println("\t\tID: " + id);
		JSONObject name = tree.getJSONObject("name");

		String name_en = null;
		if (name.has("en")) {
			Object name_en_obj = name.get("en");
			if (name_en_obj != null) {
				if (!name_en_obj.equals("null")) {
//					System.out.println(name_en_obj);
					// name_en = name.getString("en");
				}
			}
		}

		String name_de = name.getString("de");

		JSONArray children = tree.getJSONArray("children");

		if (children.length() > 0) {

			for (int i = 0; i < children.length(); i++) {
				JSONObject child = children.getJSONObject(i);
				if (child.has("id")) {
					Integer child_id = child.getInt("id");
					if (fatherChildren.containsKey(id)) {
						fatherChildren.get(id).add(child_id);
					} else {
						List<Integer> aaa = new ArrayList<>();
						aaa.add(child_id);
						fatherChildren.put(id, aaa);
					}
				}

			}

			for (int i = 0; i < children.length(); i++) {
				JSONObject child = children.getJSONObject(i);
				extractHierarchicalStructure(child, fatherChildren);
			}

		}

	}

	public Map<Integer, Organization> retrieveAllOrganizations(String folderPath, List<Source> sources,
			Map<Integer, List<Integer>> fatherChildren, Set<Integer> mainOrganizations) {

		File folder = new File(folderPath);

		Map<Integer, Organization> idOrganizations = new HashMap<>();

		System.out.println("Number of files: " + folder.listFiles().length);

		int length = folder.listFiles().length;

		int count = 0;

		for (File fileEntry : folder.listFiles()) {

			if (count % 1000 == 0) {
				System.out.println("Progress: " + count + "/" + length);
			}

			// System.out.println(fileEntry.getName());
			if (fileEntry.getName().contains("json")) {

				String fileName = fileEntry.getName();

				Integer id = Integer.parseInt(fileName.substring(0, fileName.indexOf('.')));

				String jsonContent = readJsonFile(fileEntry);

				Organization org = generateOrganizationFromJSON(jsonContent, id, sources);

				idOrganizations.put(id, org);

				if (mainOrganizations.contains(id)) {

					JSONObject main = new JSONObject(jsonContent);

//					System.out.println("MAIN: " + main.keySet().toString());

					if (main.has("institutionDetail")) {
						JSONObject o1 = main.getJSONObject("institutionDetail");
//						System.out.println("institution");
//						System.out.println("institution: " + o1.keySet().toString());

						if (o1.has("institution")) {

							JSONObject o2 = o1.getJSONObject("institution");

							if (o2.has("tree")) {
								JSONObject tree = o2.getJSONObject("tree");

								extractHierarchicalStructure(tree, fatherChildren);

							}

						}
					}
				}

				// System.out.println("Organization id: " + id + " INSERTED!");
				count++;
			}

		}

		return idOrganizations;

	}

	private Organization generateOrganizationFromJSON(String json, Integer id, List<Source> sources) {

		boolean print = false;

		if (json == null) {
			throw new RuntimeException("Null JSON!! ID: " + id);
		}

		JSONObject main = new JSONObject(json);
		Organization org = new Organization();
		org.setSources(sources);

		List<Link> links = new ArrayList<>();
		List<OrganizationIdentifier> organizationIdentifiers = new ArrayList<>();

		OrganizationIdentifier id1 = new OrganizationIdentifier();
		id1.setIdentifier(id + "");
		id1.setIdentifierName("Gerit ID");
		id1.setLink("http://www.gerit.org/en/institutiondetail/" + id);
		id1.setOrganization(org);
		id1.setProvenance("Gerit");
		id1.setVisibility(true);
		organizationIdentifiers.add(id1);
		countIdentifiers++;
		if (print) {
			System.out.println("MAIN: " + main.keySet().toString());
		}
		if (main.has("institutionDetail")) {
			JSONObject o1 = main.getJSONObject("institutionDetail");
			if (print) {
				System.out.println("institution");
				System.out.println("institution: " + o1.keySet().toString());
			}
			if (o1.has("institution")) {

				JSONObject o2 = o1.getJSONObject("institution");
				if (print) {
					System.out.println("institution: " + o2.keySet().toString());
				}
				String name = "";

				if (o2.has("name")) {
					JSONObject bildtitel = o2.getJSONObject("name");

					String name_en = "";
					if (bildtitel.get("en") != JSONObject.NULL) {
						name_en = bildtitel.getString("en");
					}
					String name_de = bildtitel.getString("de");

					if (!name_en.trim().equals("")) {
						name = name_en;
					} else {
						name = name_de;
					}

				}
				if (print) {
					System.out.println("name: " + name);
				}
				org.setLabel(name);

				org.setCountry("Germany");
				org.setCountryCode("DE");
//org.setIsPublic(isPublic);

//org.setOrganizationIdentifiers(organizationIdentifiers);

				String type = null;

				if (o2.has("_einrichtungstypText")) {

					JSONObject _einrichtungstypText = o2.getJSONObject("_einrichtungstypText");

					String type_en = _einrichtungstypText.getString("en");
					String type_de = _einrichtungstypText.getString("de");
					if (!type_en.trim().equals("")) {
						type = type_en;
					} else if (!type_de.trim().equals("")) {
						type = type_de;
					}

				}
				if (print) {
					System.out.println("type: " + type);
				}
				String crossreflink_1 = null;

				if (o2.has("crossreflink_1")) {
					if (o2.get("crossreflink_1") != JSONObject.NULL) {
						String crossreflink = o2.getString("crossreflink_1");
						if (!crossreflink.trim().equals("")) {
							crossreflink_1 = crossreflink;

							OrganizationIdentifier cross = new OrganizationIdentifier();
							cross.setIdentifier(crossreflink_1);
							cross.setIdentifierName("Crossref");
							cross.setLink("https://search.crossref.org/funding?q=" + crossreflink_1);
							cross.setOrganization(org);
							cross.setProvenance("");
							cross.setVisibility(true);
							organizationIdentifiers.add(cross);
							countIdentifiers++;
							System.out.println("CROSSREF: " + crossreflink_1);
						}
					}
				}

				String wiki = null;
				if (o2.has("wikipedia")) {
					if (o2.get("wikipedia") != JSONObject.NULL) {
						JSONObject wikipedia = o2.getJSONObject("wikipedia");
						if (wikipedia.get("en") != JSONObject.NULL) {
							String wikipedia_en = wikipedia.getString("en");
							Link l1 = new Link();
							l1.setLabel("English Wikipedia");
							l1.setSources(sources);

							l1.setType("HomePage");
							l1.setUrl(wikipedia_en);
							links.add(l1);

							if (!wikipedia_en.trim().equals("")) {
								wiki = wikipedia_en;

							}

						}
						if (wikipedia.get("en") != JSONObject.NULL) {
							String wikipedia_de = wikipedia.getString("de");

							Link l2 = new Link();

							l2.setLabel("German Wikipedia");
							l2.setSources(sources);

							l2.setType("HomePage");
							l2.setUrl(wikipedia_de);

							links.add(l2);

						}

					}

				}
				if (print) {
					System.out.println("Wikipedia: " + wiki);
				}
				String address = null;
				String city = null;
				String zipcode = null;
				String region = null;

				if (o2.has("anschrift")) {

					if (o2.get("anschrift") != JSONObject.NULL) {

						JSONObject anschrift = o2.getJSONObject("anschrift");

						city = anschrift.getString("ort");

						if (anschrift.get("plzvorort") != JSONObject.NULL) {
							zipcode = anschrift.getString("plzvorort");
						}
						if (anschrift.get("strasse") != JSONObject.NULL) {
							address = anschrift.getString("strasse");
						}

						if (anschrift.get("bundesland") != JSONObject.NULL) {
							region = anschrift.getString("bundesland");
						}
					}

				}
				if (print) {
					System.out.println("address: " + address);
					System.out.println("city: " + city);
					System.out.println("zipcode: " + zipcode);
					System.out.println("region: " + region);
				}
				org.setAddress(address);
				org.setCity(city);
				org.setPostcode(zipcode);
				org.setUrbanUnit(region);

				String grid = null;
				if (o2.has("grid")) {
					if (o2.get("grid") != JSONObject.NULL) {
						grid = o2.getString("grid");
					}

				}
				if (print) {
					System.out.println("grid: " + grid);
				}
				if (grid != null) {

					OrganizationIdentifier id2 = new OrganizationIdentifier();
					id2.setIdentifier(grid);
					id2.setIdentifierName("Grid");
					id2.setLink("https://www.grid.ac/institutes/" + grid);
					id2.setOrganization(org);
					id2.setProvenance("");
					id2.setVisibility(true);
					organizationIdentifiers.add(id2);
					countIdentifiers++;
					System.out.println("GRID: " + grid);

				}

				Double lon = null;
				Double lat = null;
				if (o2.has("geolocation")) {
					if (o2.get("geolocation") != JSONObject.NULL) {
						JSONObject geolocation = o2.getJSONObject("geolocation");

						lon = geolocation.getDouble("lon");
						lat = geolocation.getDouble("lat");

						org.setLat(lat.floatValue());
						org.setLon(lon.floatValue());
					}
				}
				if (print) {
					System.out.println("lat: " + lat + "\tlon: " + lon);
				}
				String url = null;
				if (o2.has("url")) {
					if (o2.get("url") != JSONObject.NULL) {
						url = o2.getString("url");
					}
				}

				if (print) {
					System.out.println("url: " + url);
				}
				if (url != null) {
					Link l = new Link();
					l.setLabel("HomePage");
					l.setSources(sources);

					l.setType("HomePage");
					l.setUrl(url);
					links.add(l);
				}

				org.setLinks(links);
				org.setOrganizationIdentifiers(organizationIdentifiers);

			}

		}

		return org;

	}

	private String readJsonFile(File file) {
		String content = "";

		try {

			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF8"));

			String str = null;

			while ((str = in.readLine()) != null) {
//				System.out.println(str);
				content += str + "\n";
			}

			in.close();
			return content;
		}

		catch (UnsupportedEncodingException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println(e.getMessage());
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}

		return content;
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

}
