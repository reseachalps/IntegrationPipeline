package it.unimore.alps.sources.cordis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.opencsv.bean.CsvToBeanBuilder;

public class Utility {

	public static void main(String[] args) {

		String orgsPath = "/Users/paolosottovia/Downloads/cordis-h2020organizations.csv";
		String orgsPathMod = "/Users/paolosottovia/Downloads/cordis-h2020organizations_mod.csv";
		String projsPath = "/Users/paolosottovia/Downloads/cordis-h2020projects.csv";

		String orgsPathFP7Mod = "/Users/paolosottovia/Downloads/cordis-fp7organizations_mod.csv";
		String projsPathFP7 = "/Users/paolosottovia/Downloads/cordis-fp7projects.csv";

		// List<CordisOrganization> orgs = readCordisOrganizations(orgsPath);
		// System.out.println("Number of organizations: " + orgs.size());
		//
		// List<CordisOrganization> orgsIT = filterCordisOrganizationsByCountry(orgs,
		// "IT");
		// System.out.println("Number of organizations filtered: " + orgsIT.size());
//
//		try {
//
//			List<CordisOrganization> beans = new CsvToBeanBuilder(new FileReader(orgsPathMod))
//					.withType(CordisOrganization.class).withSeparator(',').build().parse();
//			Set<String> projectIds = new HashSet<>();
//			Set<String> organizationIds = new HashSet<>();
//
//			for (CordisOrganization org : beans) {
//				projectIds.add(org.getProjectID());
//				organizationIds.add(org.getId());
//			}
//
//			System.out.println("H2020 Number of beans: " + beans.size());
//			System.out.println("H2020 Number of distincts projects: " + projectIds.size());
//			System.out.println("H2020 Number of distincts organizations: " + organizationIds.size());
//		} catch (IllegalStateException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

		try {
			List<CordisProject> beans = new CsvToBeanBuilder(new FileReader(projsPath)).withType(CordisProject.class)
					.withSeparator(';').withQuoteChar('"').withSkipLines(1).build().parse();

			System.out.println("H2020 Number of projects: " + beans.size());
			
			for(CordisProject pr:beans) {
				System.out.println("pr: "+ pr.getRcn());
			}
			
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		System.exit(0);

		try {
			List<CordisOrganization> beans = new CsvToBeanBuilder(new FileReader(orgsPathFP7Mod))
					.withType(CordisProject.class).withSeparator(',').build().parse();

			System.out.println("FP7 Number of organizations: " + beans.size());
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			List<CordisProject> beans = new CsvToBeanBuilder(new FileReader(projsPathFP7)).withType(CordisProject.class)
					.withSeparator(';').withQuoteChar('"').build().parse();

			System.out.println("FP7 Number of projects: " + beans.size());
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static Reader getReader(String file) {
		String content = null;
		//

		try {
			return new FileReader(file);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	public static List<CordisOrganization> filterCordisOrganizationsByCountry(List<CordisOrganization> orgs,
			String country) {
		List<CordisOrganization> results = new ArrayList<>();
		for (CordisOrganization o : orgs) {
			if (o.getCountry().equals(country)) {
				results.add(o);
			}
		}
		return results;
	}

	public static List<CordisOrganization> readCordisOrganizations(String path) {
		Reader in;
		List<CordisOrganization> organizations = new ArrayList<>();

		return organizations;
		// return readCSVOrganizations(path);
	}

	private static CordisOrganization getOrganizationFromStrings(List<String> items) {
		if (items.size() < 23) {
			System.err.println("ERROR ITEM: " + items.toString());
			return null;
		}

		return new CordisOrganization(items.get(0), items.get(1), items.get(2), items.get(3), items.get(4),
				items.get(5), items.get(6), items.get(7), items.get(8), items.get(9), items.get(10), items.get(11),
				items.get(12), items.get(13), items.get(14), items.get(15), items.get(16), items.get(17), items.get(18),
				items.get(19), items.get(20), items.get(21), items.get(22));
	}

	public static List<CordisProject> readCordisProject(String path) {

		List<List<String>> projects = readCSV(path, ";(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		List<CordisProject> cordisProjects = new ArrayList<>();
		for (List<String> prj : projects) {

			CordisProject p = getProjectFromStrings(prj);

			if (p != null) {
				cordisProjects.add(p);
			}
		}

		return cordisProjects;
	}

	private static CordisProject getProjectFromStrings(List<String> items) {
		if (items.size() < 21) {
			System.err.println("ERROR ITEM: " + items.toString());
			return null;
		}

		return new CordisProject(items.get(0), items.get(1), items.get(2), items.get(3), items.get(4), items.get(5),
				items.get(6), items.get(7), items.get(8), items.get(9), items.get(10), items.get(11), items.get(12),
				items.get(13), items.get(14), items.get(15), items.get(16), items.get(17), items.get(18), items.get(19),
				items.get(20));
	}

	public static List<CordisOrganization> readCSVOrganizations(String path) {
		try {
			List<CordisOrganization> beans = new CsvToBeanBuilder(new FileReader(path))
					.withType(CordisOrganization.class).withSeparator(';').withIgnoreQuotations(false)
					.withQuoteChar('\"').build().parse();

			return beans;

		} catch (IllegalStateException | FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	public static List<List<String>> readCSV(String path, String separator) {
		List<List<String>> lines = new ArrayList<>();
		String completeDoc = "";
		try {
			File fileDir = new File(path);

			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(fileDir), "UTF8"));

			String str;
			int count = 0;
			while ((str = in.readLine()) != null) {
				// System.out.println(str);
				if (!str.trim().equals("")) {
					// completeDoc += str + "\n";
					String[] elements = str.split(separator, -1);
					if (count % 1000 == 0) {
						System.out.println(count);
					}
					// System.out.println("Number of elements: " + elements.length);
					List<String> line = new ArrayList<>();
					for (String element : elements) {
						line.add(element);
					}
					lines.add(line);

				}
				count++;

			}

			in.close();
			System.out.println("HERE");

			String content = new String(Files.readAllBytes(Paths.get(fileDir.getAbsolutePath())));

			String[] splits = content.split(separator, -1);

			System.out.println("Number of elems: " + splits.length);

			System.out.println("" + splits.toString());

		} catch (UnsupportedEncodingException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println(e.getMessage());
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}

		return lines;
	}

	// public static List<List<String>> readCSV(String path, String separator) {
	// List<List<String>> lines = new ArrayList<>();
	//
	// try {
	// File fileDir = new File(path);
	//
	// BufferedReader in = new BufferedReader(new InputStreamReader(new
	// FileInputStream(fileDir), "UTF8"));
	//
	// String str;
	//
	// while ((str = in.readLine()) != null) {
	// // System.out.println(str);
	// if (!str.trim().equals("")) {
	// String[] elements = str.split(separator, -1);
	// // System.out.println("Number of elements: " + elements.length);
	// List<String> line = new ArrayList<>();
	// for (String element : elements) {
	// line.add(element);
	// }
	// lines.add(line);
	//
	// }
	//
	// }
	//
	// in.close();
	// } catch (UnsupportedEncodingException e) {
	// System.out.println(e.getMessage());
	// } catch (IOException e) {
	// System.out.println(e.getMessage());
	// } catch (Exception e) {
	// System.out.println(e.getMessage());
	// }
	//
	// return lines;
	// }

}
