package it.unimore.alps.sources.gerit;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import it.unimore.alps.sql.model.AlternativeName;
import it.unimore.alps.sql.model.Project;

public class CrawlerProject {

	public static void main(String[] args) {
		CrawlerProject cp = new CrawlerProject();

		Map<String, String> cookies = new HashMap<>();

		cookies.put("WT_FPC", "id=10.8.5.23-1892513248.30705212:lv=1551781470789:ss=1551781470789");
		cookies.put("WT_FPC", "id=10.8.5.23-1892513248.30705212:lv=1552393037515:ss=1552393030323");
		cookies.put("WEBTRENDS_ID", "10.8.5.23-1892513248.30705212");
		cookies.put("TS01ece210",
				"017464b986cfc58074c40b9c2a2e7ca494beca02d8f6137e41f2680ef635cb98207ffacf44b833528a99cb3099e4dacd0a2641eef0b0561898c9eb0bc900e487055511aa55");
		cookies.put("TS010a8d8b",
				"017464b9867e9b2cf60c1c19ca8239c9da5e117fa06cfb273b4c57ca9bc0f05926804e0b532729f8be8dad0b41774f230185c5dd15");
		cookies.put("JSESSIONID", "9440AFE71E04D5EE68B5A5F152179026");
		cookies.put("BIGipServer~web~iApp_SDC.app~iApp_SDC_pool", "218368010.20480.0000");

		Map<Integer, List<Integer>> organizationProjects = new HashMap<>();
		Map<Integer, DFGProject> idProjects = new HashMap<>();

		String folderOutpath = "/Users/paolosottovia/Downloads/projectsGerit/";
		Set<Integer> alreadyProcessedIds = cp.retrieveOrganizationDone(new File(folderOutpath));

		File folder = new File("/Users/paolosottovia/eclipse-workspace/crawler/jsonData/");
		List<Integer> ids = cp.retrieveAllOrganizationIds(folder);

		System.out.println("Number of IDS: " + ids.size());

		for (Integer id : ids) {

			// cp.downloadProjectsFromOrganizationID(10045, organizationProjects,
			// idProjects);
			if (!alreadyProcessedIds.contains(id)) {
				cp.downloadProjectsFromOrganizationID(id, organizationProjects, idProjects, cookies);
			} else {
				System.err.println("Already downloaded id: " + id);
			}
		}
	}

	public Set<Integer> retrieveOrganizationDone(File folder) {
		Set<Integer> ids = new HashSet<>();
		for (File file : folder.listFiles()) {
			String filename = file.getName();

			if (filename.contains(".json")) {
				Integer id = Integer.parseInt(filename.replace(".json", ""));
				ids.add(id);
			}
		}

		return ids;

	}

	public List<Integer> retrieveAllOrganizationIds(File folder) {
		// Map<Integer, Organization> idOrganizations = new HashMap<>();

		List<Integer> list = new ArrayList<>();

		System.out.println("Number of files: " + folder.listFiles().length);

		int length = folder.listFiles().length;

		int count = 0;

		for (File fileEntry : folder.listFiles()) {

			if (count % 1000 == 0) {
				System.out.println("Progress: " + count + "/" + length);
			}
			if (fileEntry.getName().contains(".json")) {
				list.add(Integer.parseInt(fileEntry.getName().replace(".json", "")));
			}

		}

		return list;
	}

	public void downloadProjectsFromOrganizationID(Integer id, Map<Integer, List<Integer>> organizationProjects,
			Map<Integer, DFGProject> idProjects, Map<String, String> cookies) {

		String baseURL = "http://gepris.dfg.de";

		String institutionURL = "/gepris/institution/";

		String language = "?language=en";

		String url = baseURL + institutionURL + id + language;

		String result = null;

		System.out.println("Crawling url: " + url);

		Document doc;
		try {

			String script = null;
			// String url = "http://www.gerit.org/en/institutiondetail/10045";

			doc = Jsoup.connect(url).header("Accept-Language", "en-US,en;q=0.9,it;q=0.8,fr;q=0.7").header("User-Agent",
					"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36")
					.cookies(cookies).get();

			// Connection a = Jsoup.connect(url);

//			System.out.println(doc.toString());

			// String classPrjs = "jstree-no-dots jstree-no-icons";

			String idProject__ = "projekteNachProgrammen";

			Element projectListElement = doc.getElementById(idProject__);

			// System.out.println("ELEMENT: " + projectListElement.toString());

			if (projectListElement != null) {
				Elements projectList = projectListElement.getElementsByClass("intern hrefWithNewLine");

				System.out.println("LIST:");
				List<Integer> projectListIds = new ArrayList<Integer>();

				for (Element el : projectList) {

//				System.out.println("PRJ: " + el.toString());

					String project_path = el.attr("href");

					String project_id = project_path.replace("/gepris/projekt/", "");
					Integer project_id_INTEGER = Integer.parseInt(project_id);
					projectListIds.add(project_id_INTEGER);

					String project_url = baseURL + el.attr("href") + "?language=en";
					String project_name = el.text();

//				System.out.println("project_id: " + project_id);
//				System.out.println("name: " + project_name);
//				System.out.println("project url: " + project_url);
					if (!idProjects.containsKey(project_id_INTEGER)) {
						DFGProject p = downloadProjectFromID(project_name, project_url, project_id, cookies);
						System.out.println("P: " + p.toString());
						idProjects.put(project_id_INTEGER, p);
					}
				}

				organizationProjects.put(id, projectListIds);

				saveProjects("/Users/paolosottovia/Downloads/projectsGerit/", id, organizationProjects, idProjects);
			} else {
				System.out.println("No Project for organization: " + id);
			}

		} catch (HttpStatusException e) {
			System.out.println("ERROR 404");

			// e.printStackTrace();
			// return null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// return result;

	}

	private void saveProjects(String folder, Integer id, Map<Integer, List<Integer>> organizationProjects,
			Map<Integer, DFGProject> idProjects) {

		String fileName = id + ".json";
		List<String> lines = new ArrayList<>();
		for (Integer project_id : organizationProjects.get(id)) {

			DFGProject prj = idProjects.get(project_id);
			// if (prj)

			lines.add(prj.toJSONOBject().toString());

		}

		Writer out;
		try {
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(folder + fileName), "UTF-8"));

			for (String line : lines) {
				out.write(line + "\n");
			}

			out.close();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private DFGProject downloadProjectFromID(String name, String url, String id, Map<String, String> cookies) {

		String result = null;

		System.out.println("Crawling url: " + url);

		Document doc;
		try {

			String script = null;
			// String url = "http://www.gerit.org/en/institutiondetail/10045";

			doc = Jsoup.connect(url).header("Accept-Language", "en-US,en;q=0.9,it;q=0.8,fr;q=0.7").header("User-Agent",
					"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36")
					.cookies(cookies).get();

			// Connection a = Jsoup.connect(url);

//			System.out.println(doc.toString());

			// String classPrjs = "jstree-no-dots jstree-no-icons";

			String mainClass = "content_inside detailed";

			Elements els = doc.getElementsByClass(mainClass);

			for (Element el : els) {
//				System.out.println(el.toString());
//				System.out.println("------------------------");

				System.out.println("Name: " + name);

				Elements details = el.getElementsByClass("details");

				{
					Map<String, String> keys = new HashMap<>();
					Element detail = details.get(0);

					Elements divs = detail.getElementsByTag("div");

					for (Element div : divs) {
						// System.out.println("\t\t" + div.toString());
						String key = div.getElementsByClass("name").text();
						String v = div.getElementsByClass("value").text();

//						System.out.println("key: " + key);
//						System.out.println("v: " + v);

						keys.put(key, v);

					}

					Elements e = el.select(".details > div:nth-child(3)");

//				System.out.println("\tNumber of elements: " + e.size());

					String duration = "";

					if (keys.containsKey("Förderung")) {
						duration = keys.get("Förderung");
					}
					if (keys.containsKey("Term")) {
						duration = keys.get("Term");
					}

					// e.get(0).getElementsByClass("value").text();

					String prj_url = "";

					Elements e1 = el.select(".details > div:nth-child(4)");

//				System.out.println("E1: " + e1.toString());

					if (e1 != null) {
						if (e1.size() > 0) {

							prj_url = e1.get(0).getElementsByClass("extern").attr("href");
						}

					}
					System.out.println("\tduration: " + duration);

					String startDate = getStartDate(duration);
					String endDate = getEndDate(duration);

					Integer monthsDuration = 0;

					if (startDate != null) {

						if (endDate != null) {

							Integer sDate = Integer.parseInt(startDate);
							Integer eDate = Integer.parseInt(endDate);

							monthsDuration = (eDate - sDate) * 12;

						} else {

							Integer sDate = Integer.parseInt(startDate);
							Integer eDate = 2019;

							monthsDuration = (eDate - sDate) * 12;

						}
					} else {
						System.err.println("NuLL start date: " + duration);
					}

//				System.out.println("Start date: " + startDate);
//				System.out.println("End date: " + endDate);
//				System.out.println("Months duration: " + monthsDuration);
//
//				System.out.println("\tproject url: " + prj_url);

					String description = el.getElementById("projekttext").text();

//				System.out.println("\tdescription: " + description);

					String programme = el.select("#projektbeschreibung > div:nth-child(2)").get(0).text();
//
//				System.out.println("\tprogramme: " + programme);

					DFGProject prj = new DFGProject(id, name, programme, description, prj_url, monthsDuration + "",
							startDate, endDate);

					return prj;

				}
			}

			// System.out.println("EXIT");
			// System.exit(0);

		} catch (HttpStatusException e) {
			System.out.println("ERROR 404");

			// e.printStackTrace();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;

	}

	private String getStartDate(String duration) {

		if (duration.contains("Förderung von ")) {

			return duration.replace("Förderung von ", "").substring(0, 4);

		} else if (duration.contains("Förderung seit ")) {

			return duration.replace("Förderung seit ", "");

		} else if (duration.contains("since ")) {

			return duration.replace("since ", "");

		} else if (duration.contains("from ")) {

			return duration.replace("from ", "").substring(0, 4);

		} else if (duration.contains("Funded in ")) {
			return duration.replace("Funded in ", "").substring(0, 4);
		}

		return null;

	}

	private String getEndDate(String duration) {

		if (duration.contains("Förderung von ")) {

			return duration.substring(duration.indexOf("bis ") + "bis ".length());

		} else if (duration.contains("Förderung seit ")) {

			return null;

		} else if (duration.contains("since ")) {

			return null;

		} else if (duration.contains("from ")) {

			return duration.substring(duration.indexOf("to ") + "to ".length());

		}

		return null;

	}

}
