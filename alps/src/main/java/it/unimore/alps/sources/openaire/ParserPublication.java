package it.unimore.alps.sources.openaire;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.PropertyException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;

import org.openarchives.oai._2.HeaderType;
import org.openarchives.oai._2.ListRecordsType;
import org.openarchives.oai._2.OAIPMHtype;
import org.openarchives.oai._2.RecordType;

import eu.openaire.namespace.oaf.CategoryType;
import eu.openaire.namespace.oaf.ChildrenOrg;
import eu.openaire.namespace.oaf.ChildrenResult;
import eu.openaire.namespace.oaf.ClassedSchemedElement;
import eu.openaire.namespace.oaf.ConceptType;
import eu.openaire.namespace.oaf.ContextType;
import eu.openaire.namespace.oaf.DatainfoType;
import eu.openaire.namespace.oaf.Entity;
import eu.openaire.namespace.oaf.ExternalreferenceType;
import eu.openaire.namespace.oaf.FunderFlatType;
import eu.openaire.namespace.oaf.FundingFlatType;
import eu.openaire.namespace.oaf.InferenceExtendedStringType;
import eu.openaire.namespace.oaf.InstanceType;
import eu.openaire.namespace.oaf.JournalType;
import eu.openaire.namespace.oaf.NamedFundingLevel;
import eu.openaire.namespace.oaf.NamedIdElement;
import eu.openaire.namespace.oaf.OptionalClassedSchemedElement;
import eu.openaire.namespace.oaf.RelToType;
import eu.openaire.namespace.oaf.RelType;
import eu.openaire.namespace.oaf.RelsType;
import eu.openaire.namespace.oaf.ResultChildrenType;
import eu.openaire.namespace.oaf.WebresourceType;

public class ParserPublication {

	public static void main(String[] args) {

		Set<String> ids = new HashSet<>();
		ids.add("311166");

		// List<Map<String, Object>> publications = getPublicationsFromFilesByProject(
		// "/Users/paolosottovia/Downloads/publications/", "publications_", ids);

		List<Map<String, Object>> publications = getPublicationsFromFilesByCodePresence(
				"/Users/paolosottovia/Downloads/publications/", "publications_");

		for (Map<String, Object> pub : publications.subList(0, 10)) {
			System.out.println("------------------------------------------------------------------------------------");
			visit(pub, 0);
			String openaire_identifier = (String) pub.get("openaire_identifier");
			System.out.println("openaire identifier: " + openaire_identifier);
			// check for publicaiton id

			// String

			String dateofacceptance = (String) ((Map<String, Object>) pub.get("dateofacceptance")).get("value");
			String description = (String) pub.get("description");
			String title = (String) ((Map<String, Object>) pub.get("title")).get("content");

			if (pub.containsKey("journal")) {
				String location_name = (String) ((Map<String, Object>) pub.get("journal")).get("value");
			}

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

			for (Map<String, Object> rel : (List<Map<String, Object>>) pub.get("rels")) {
				System.out.println(rel.toString() + "\n");
				if (rel.containsKey("ranking")) {
					Map<String, Object> to = (Map<String, Object>) rel.get("to");
					if (to.containsKey("clazz")) {
						String clazz = (String) to.get("clazz");
						if (clazz.equals("hasAuthor")) {

							String personIdentifier = (String) to.get("value");

							String personFullName = (String) rel.get("fullname");

							System.out.println(personIdentifier + "\t" + personFullName);

						}
					}

				}
			}

		}

		System.out.println("Number of publications: " + publications.size());

		Map<String, List<Map<String, Object>>> codePublications = getPublicationByCordisID(publications);

		System.out.println("Number of projects: " + codePublications);

	}

	public static Map<String, List<Map<String, Object>>> getPublicationByCordisID(
			List<Map<String, Object>> publications) {

		Map<String, List<Map<String, Object>>> codePublications = new HashMap<>();

		for (Map<String, Object> publication : publications) {
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
		return codePublications;
	}

	public static Set<String> getProjectsFromOrganizations(List<Map<String, Object>> organizations) {
		Set<String> ids = new HashSet<>();
		for (Map<String, Object> organization : organizations) {

			if (organization.containsKey("rels")) {
				List<Map<String, Object>> rels = (List<Map<String, Object>>) organization.get("rels");

				for (Map<String, Object> rel : rels) {
					String s = "to";
					Map<String, Object> a = (Map<String, Object>) rel.get("to");

					String id = (String) a.get("value");
					ids.add(id);
				}

			}
		}

		return ids;
	}

	public static Map<String, List<Map<String, Object>>> getOrganizationsFromFilesByOriginalID(String inputFolder,
			String prefix) {
		Map<String, List<Map<String, Object>>> results = new HashMap<>();

		List<String> files = Utils.retrievePaths(inputFolder, prefix);
		for (String file : files) {
			results.putAll(filterOrganizationsByOriginalId(file));

		}

		return results;
	}

	public static List<Map<String, Object>> getPublicationsFromFilesByProject(String inputFolder, String prefix,
			Set<String> ids, boolean TEST) {
		List<Map<String, Object>> results = new ArrayList<>();

		List<String> files = Utils.retrievePaths(inputFolder, prefix);
		int n = 0;

		if (TEST) {
			files = files.subList(0, 100);
			System.err.println("TEST MODE!!!!!");
		}

		List<SinglePublicationFileReader> tasks = new ArrayList<>();

		for (String file : files) {
			// results.addAll(filterPublicationByProjectId(file, ids));

			tasks.add(new SinglePublicationFileReader(file, ids, results));
			// System.out.println("\t" + n + "/" + files.size() + "\t" + file + "\tpartial
			// results: " + results.size());
			n++;
		}

		ExecutorService executorService2 = Executors.newFixedThreadPool(20);

		for (SinglePublicationFileReader task : tasks) {
			executorService2.submit(task);
		}
		executorService2.shutdown();

		try {
			executorService2.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			System.err.println(e.toString());
		}

		return results;
	}

	public static List<Map<String, Object>> getPublicationsFromFilesByCodePresence(String inputFolder, String prefix) {
		List<Map<String, Object>> results = new ArrayList<>();

		List<String> files = Utils.retrievePaths(inputFolder, prefix);
		for (String file : files) {
			results.addAll(filterPublicationByProjectIdPresence(file));

		}

		return results;
	}

	public static List<Map<String, Object>> getOrganizationsFromFilesByName(String inputFolder, String prefix,
			Set<String> names) {
		List<Map<String, Object>> results = new ArrayList<>();

		List<String> files = Utils.retrievePaths(inputFolder, prefix);
		for (String file : files) {
			results.addAll(filterOrganizationsByName(file, names));

		}

		return results;
	}

	public static Set<String> intesection(Set<String> set1, Set<String> set2) {
		Set<String> set = new HashSet<>();

		for (String s1 : set1) {
			if (set2.contains(s1)) {
				set.add(s1);
			}
		}
		return set;
	}

	public static String getWebsiteURL(Map<String, Object> organization) {
		String url = (String) organization.get("websiteurl");

		// System.out.println("URL: " + url);

		return url;

	}

	public static Set<String> getProjectIds(Map<String, Object> organization) {

		List<Map<String, Object>> l = (List<Map<String, Object>>) organization.get("rels");
		Set<String> values = new HashSet<>();
		for (Map<String, Object> el : l) {
			// System.out.println(el.toString());

			List<Map<String, Object>> ToOrTitleOrWebsiteurl = (List<Map<String, Object>>) el
					.get("ToOrTitleOrWebsiteurl");
			for (Map<String, Object> map : ToOrTitleOrWebsiteurl) {
				if (map.containsKey("to")) {
					Map<String, Object> to = (Map<String, Object>) map.get("to");
					String value = (String) to.get("value");
					// System.out.println("Value: " + value);
					values.add(value);
				}
			}
		}
		return values;
	}

	public static List<Map<String, Object>> filterOrganizationsByName(String path, Set<String> names) {

		List<Map<String, Object>> filesOrganizations = retriveOrganizationFromXmlFile(path);
		List<Map<String, Object>> organizationsFiltered = new ArrayList<>();

		for (Map<String, Object> organization : filesOrganizations) {

			String legalname = (String) organization.get("legalname");

			// System.out.println(country.keySet().toString());

			if (names.contains(legalname)) {
				organizationsFiltered.add(organization);
			}

		}

		return organizationsFiltered;

	}

	public static Map<String, List<Map<String, Object>>> filterGivenOrganizationsByOriginalId(
			List<Map<String, Object>> filesOrganizations) {
		Map<String, List<Map<String, Object>>> organizationsMap = new HashMap<>();
		for (Map<String, Object> organization : filesOrganizations) {

			// Map<String, Object> country = (Map<String, Object>)
			// organization.get("country");

			if ((organization.get("originalId")) instanceof String) {
				String id = ((String) organization.get("originalId"));
				if (id.contains("::")) {
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
				System.out.println(map.toString());
			}

		}

		return organizationsMap;
	}

	public static Map<String, List<Map<String, Object>>> filterOrganizationsByOriginalId(String path) {

		List<Map<String, Object>> filesOrganizations = retriveOrganizationFromXmlFile(path);
		Map<String, List<Map<String, Object>>> organizationsMap = new HashMap<>();

		for (Map<String, Object> organization : filesOrganizations) {

			// Map<String, Object> country = (Map<String, Object>)
			// organization.get("country");

			if ((organization.get("originalId")) instanceof String) {
				String id = ((String) organization.get("originalId"));
				if (id.contains("::")) {
					id = id.substring(id.indexOf("::"));
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
				System.out.println(map.toString());
			}

		}

		return organizationsMap;

	}

	public static List<Map<String, Object>> filterPublicationByProjectId(String path, Set<String> projectIds) {

		List<Map<String, Object>> filesPublications = retriveOrganizationFromXmlFile(path);
		List<Map<String, Object>> publicationsFiltered = new ArrayList<>();

		for (Map<String, Object> publication : filesPublications) {
			if (publication.containsKey("rels")) {
				List<Map<String, Object>> rels = (List<Map<String, Object>>) publication.get("rels");

				for (Map<String, Object> rel : rels) {
					if (rel.containsKey("code")) {
						String code = (String) rel.get("code");
						if (projectIds.contains(code)) {
							publicationsFiltered.add(publication);
						}
					}
				}
			}

		}

		return publicationsFiltered;

	}

	public static List<Map<String, Object>> filterPublicationByProjectIdPresence(String path) {

		List<Map<String, Object>> filesPublications = retriveOrganizationFromXmlFile(path);
		List<Map<String, Object>> publicationsFiltered = new ArrayList<>();

		for (Map<String, Object> publication : filesPublications) {
			if (publication.containsKey("rels")) {
				List<Map<String, Object>> rels = (List<Map<String, Object>>) publication.get("rels");

				for (Map<String, Object> rel : rels) {
					if (rel.containsKey("code")) {
						// String code = (String) rel.get("code");
						// if (projectIds.contains(code)) {
						publicationsFiltered.add(publication);
						// }
					}
				}
			}

		}

		return publicationsFiltered;

	}

	public static List<Map<String, Object>> retriveOrganizationFromXmlFile(String path) {

		try {

			JAXBContext jc = JAXBContext.newInstance(
					"org.openarchives.oai._2:eu.openaire.namespace.oaf:eu.driver_repository.namespace.dri");
			Unmarshaller unmarshaller = jc.createUnmarshaller();
			// String path = "output/out_2.xml";
			File file = new File(path);
			URL url = file.toURI().toURL();
			InputStream xml = url.openStream();
			JAXBElement<OAIPMHtype> feed = unmarshaller.unmarshal(new StreamSource(xml), OAIPMHtype.class);
			xml.close();

			ListRecordsType list = feed.getValue().getListRecords();

			List<RecordType> records = list.getRecord();

			// System.out.println("Number of record: " + records.size());
			List<Map<String, Object>> orgs = new ArrayList<>();

			// test only one entity

			// for (RecordType r : records) {

			int NUM_RECORDS = records.size();
			for (int i = 0; i < NUM_RECORDS; i++) {
				RecordType r = records.get(i);
				HeaderType header = r.getHeader();
				Entity ent = (Entity) r.getMetadata().getAny();
				eu.openaire.namespace.oaf.Result res = ent.getResult();

				List<Map<String, Object>> listJAXBElement = parsePublication(res);

				String identifier = header.getIdentifier();
				// System.out.println("identifier: " + identifier);
				Map<String, Object> organization = compressOrganization(listJAXBElement);
				organization.put("openaire_identifier", identifier);
				orgs.add(organization);

			}
			return orgs;

			// Marshaller marshaller = jc.createMarshaller();
			// marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
			// marshaller.marshal(feed, System.out);

		} catch (PropertyException e) {
			e.printStackTrace();
		} catch (JAXBException e) {
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	public static String extractWebSite(Map<String, Object> organization) {
		String identifier = (String) organization.get("openaire_identifier");
		String legalname = (String) organization.get("legalname");
		String legalshortname = (String) organization.get("legalshortname");
		String website = (String) organization.get("websiteurl");
		String country = (String) ((Map<String, Object>) organization.get("country")).get("classid");
		String line = identifier + "\t" + legalname + "\t" + legalshortname + "\t" + website + "\t" + country;
		return line;
		// System.out.println(line);
	}

	public static Map<String, Object> compressOrganization(List<Map<String, Object>> organization) {
		Set<String> keySet = new HashSet<>();
		List<String> keyList = new ArrayList<>();
		for (Map<String, Object> field : organization) {

			// System.out.println("" + field.keySet());
			keySet.addAll(field.keySet());
			keyList.addAll(field.keySet());
		}
		Map<String, Object> map = new HashMap<>();

		if (keySet.size() == organization.size()) {
			for (Map<String, Object> field : organization) {
				map.putAll(field);

			}
		} else {

			for (Map<String, Object> organi : organization) {
				for (Map.Entry<String, Object> entry : organi.entrySet()) {
					String key = entry.getKey();
					Object obj = entry.getValue();
					if (map.containsKey(key)) {
						if (map.get(key) instanceof List<?>) {

							((List<Map<String, Object>>) map.get(key)).add(organi);
						} else {
							Map<String, Object> m = new HashMap<>();
							m.put(key, organi);
							map.remove(key);
							map.put(key, m);
						}
					} else {
						map.put(key, obj);
					}
				}
			}

			// System.err.println("" + keyList.size() + "\t" + keyList.toString());
			// System.err.println("" + keySet.size() + "\t" + keySet.toString());
			// throw new RuntimeException("Errore!");
		}
		return map;
	}

	public static void parseOurStructure(List<Map<String, Object>> organization) {
		Set<String> keySet = new HashSet<>();

		for (Map<String, Object> field : organization) {

			System.out.println("" + field.keySet());
			keySet.addAll(field.keySet());
		}
		Map<String, Object> map = new HashMap<>();

		if (keySet.size() == organization.size()) {
			for (Map<String, Object> field : organization) {
				map.putAll(field);
			}
		} else {
			throw new RuntimeException("Errore!");
		}

		visit(map, 0);

	}

	public static void visit(Map<String, Object> structure, int level) {
		for (Map.Entry<String, Object> entry : structure.entrySet()) {
			Object object = entry.getValue();
			String name = entry.getKey();
			System.out.println("" + getTab(level) + name);
			if (object == null) {
				System.out.println(getTab(level) + "\tnull");
			} else {
				if (object instanceof String) {
					String value = (String) object;
					System.out.println("\t" + getTab(level) + value);
				} else if (object instanceof Map<?, ?>) {
					Map<String, Object> value = (Map<String, Object>) object;
					visit(value, level + 1);
				} else if (object instanceof List<?>) {
					List<Map<String, Object>> values = (List<Map<String, Object>>) object;
					for (Map<String, Object> value : values) {
						visit(value, level + 1);
					}

				}
			}
		}
	}

	private static String getTab(int number) {
		String tabs = "\t";
		for (int i = 0; i < number; i++) {
			tabs += "\t";
		}
		return tabs;
	}

	public static List<Map<String, Object>> parsePublication(eu.openaire.namespace.oaf.Result publication) {

		List<JAXBElement<?>> a = publication.getSubjectOrTitleOrDateofacceptance();
		List<Map<String, Object>> listJAXBElement = new ArrayList<>();
		for (JAXBElement<?> af : a) {
			Map<String, Object> elementMap = extractJAXBElement(af);
			listJAXBElement.add(elementMap);
		}

		return listJAXBElement;
	}

	public static List<Map<String, Object>> parseChildrenOrganization(
			eu.openaire.namespace.oaf.ChildrenOrg.Organization organization) {

		List<JAXBElement<?>> a = organization.getLegalshortnameOrLegalnameOrWebsiteurl();
		List<Map<String, Object>> listJAXBElement = new ArrayList<>();
		for (JAXBElement<?> af : a) {
			Map<String, Object> elementMap = extractJAXBElement(af);
			listJAXBElement.add(elementMap);
		}
		Map<String, Object> elementObjectId = new HashMap<>();
		elementObjectId.put("objidentifier", organization.getObjidentifier());
		listJAXBElement.add(elementObjectId);

		return listJAXBElement;

	}

	public static Map<String, Object> extractJAXBElement(JAXBElement<?> element) {

		Map<String, Object> result = new HashMap<>();
		String name = element.getName().toString();

		// System.out.println("NAME: " + name);
		Object obj = element.getValue();

		if (obj instanceof String) {
			String value = (String) obj;
			// System.out.println("\t" + value);
			result.put(name, value);

		} else if (obj instanceof OptionalClassedSchemedElement) {
			OptionalClassedSchemedElement value = (OptionalClassedSchemedElement) obj;
			Map<String, Object> optionalClassedSchemedElementMap = parseOptionalClassedSchemedElement(value);
			result.put(name, optionalClassedSchemedElementMap);
		} else if (obj instanceof DatainfoType) {
			DatainfoType value = (DatainfoType) obj;
			// System.out.println("-----------------------------------------------");
			// System.out.println("\tDataInfo");

			List<JAXBElement<?>> elems = value.getInferredOrDeletedbyinferenceOrTrust();
			List<Map<String, Object>> list = new ArrayList<>();
			for (JAXBElement<?> elem : elems) {
				// System.out.println("\t\tname: " + elem.getName().toString() + " value: " +
				// elem.getValue().toString());
				list.add(extractJAXBElement(elem));
			}
			result.put(name, list);

		} else if (obj instanceof RelsType) {
			RelsType value = (RelsType) obj;
			List<Map<String, Object>> relsTypeMap = parseRelsType(value);
			result.put(name, relsTypeMap);

		} else if (obj instanceof ChildrenOrg) {
			ChildrenOrg value = (ChildrenOrg) obj;
			List<eu.openaire.namespace.oaf.ChildrenOrg.Organization> orgs = value.getOrganization();
			// System.out.println("\t Number of children organizations: " + orgs.size());
			List<Map<String, Object>> orgsList = new ArrayList<>();
			for (eu.openaire.namespace.oaf.ChildrenOrg.Organization org : orgs) {
				//
				// todo parse orgs recursevely
				List<Map<String, Object>> orgsMap = parseChildrenOrganization(org);
				Map<String, Object> orgsMapCompress = compressOrganization(orgsMap);
				orgsList.add(orgsMapCompress);
			}
			result.put(name, orgsList);

		} else if (obj instanceof NamedIdElement) {
			NamedIdElement value = (NamedIdElement) obj;
			Map<String, Object> nameIdElementMap = parseNamedIdElement(value);
			result.put(name, nameIdElementMap);
		} else if (obj instanceof Boolean) {
			Boolean value = (Boolean) obj;
			result.put(name, value.booleanValue());
		} else if (obj instanceof ClassedSchemedElement) {
			ClassedSchemedElement value = (ClassedSchemedElement) obj;
			Map<String, Object> classedSchemedElementMap = parseClassedSchemedElement(value);
			result.put(name, classedSchemedElementMap);
		} else if (obj instanceof ResultChildrenType) {
			ResultChildrenType value = (ResultChildrenType) obj;
			Map<String, Object> resultChildrenTypeMap = parseResultChildrenType(value);
			result.put(name, resultChildrenTypeMap);
		} else if (obj instanceof InferenceExtendedStringType) {
			InferenceExtendedStringType value = (InferenceExtendedStringType) obj;
			Map<String, Object> inferenceExtendedStringTypeMap = parseInferenceExtendedStringType(value);
			result.put(name, inferenceExtendedStringTypeMap);
		}

		else if (obj instanceof ClassedSchemedElement) {
			ClassedSchemedElement value = (ClassedSchemedElement) obj;
			Map<String, Object> classedSchemedElementMap = parseClassedSchemedElement(value);
			result.put(name, classedSchemedElementMap);
		} else if (obj instanceof JournalType) {
			JournalType value = (JournalType) obj;

			String eissn = value.getEissn();
			String issn = value.getIssn();
			String lissn = value.getLissn();
			String val = value.getValue();

			Map<String, Object> map = new HashMap<>();
			map.put("eissn", eissn);
			map.put("issn", issn);
			map.put("lissn", lissn);
			map.put("value", val);

			result.put(name, map);
		} else if (obj instanceof WebresourceType) {
			WebresourceType value = (WebresourceType) obj;

			String url = value.getUrl();

			Map<String, Object> map = new HashMap<>();

			map.put("url", url);

			result.put(name, map);

		} else if (obj instanceof ContextType) {
			ContextType value = (ContextType) obj;

			List<CategoryType> categories = value.getCategory();
			for (CategoryType type : categories) {

			}
			List<Map<String, Object>> categoriesMap = parseCategories(categories);

			String id = value.getId();
			String inferenceprovenance = value.getInferenceprovenance();
			String label = value.getLabel();
			String trust = value.getTrust();
			String type = value.getType();

			Map<String, Object> map = new HashMap<>();
			map.put("id", id);
			map.put("inferenceprovenance", inferenceprovenance);
			map.put("label", label);
			map.put("trust", trust);
			map.put("type", type);
			map.put("categories", categoriesMap);

			result.put(name, map);

		}

		else {
			System.out.println("OTHER TYPE: " + obj.getClass().getName());
			result.put(name, null);
		}

		return result;

	}

	private static List<Map<String, Object>> parseCategories(List<CategoryType> categories) {

		List<Map<String, Object>> cats = new ArrayList<>();

		for (CategoryType category : categories) {

			List<ConceptType> conceptTypes = category.getConcept();
			Map<String, Object> map = new HashMap<>();
			String id = category.getId();
			String inferenceprovenance = category.getInferenceprovenance();
			String label = category.getLabel();
			String trust = category.getTrust();

			map.put("id", id);
			map.put("inferenceprovenance", inferenceprovenance);
			map.put("label", label);
			map.put("trust", trust);

			cats.add(map);

		}
		return cats;
	}

	private static Map<String, Object> parseInferenceExtendedStringType(InferenceExtendedStringType value) {

		Map<String, Object> result = new HashMap<>();
		result.put("inferenceprovenance", value.getInferenceprovenance());
		result.put("trust", value.getTrust());
		result.put("value", value.getValue());

		return result;
	}

	private static Map<String, Object> parseResultChildrenType(ResultChildrenType value) {
		Map<String, Object> results = new HashMap<>();

		List<ExternalreferenceType> externalReferences = value.getExternalreference();
		List<InstanceType> instanceTypes = value.getInstance();
		List<ChildrenResult> childrenResults = value.getResult();

		results.put("externalreference", parseExternalReferences(externalReferences));
		results.put("instance", parseInstanceTypes(instanceTypes));
		results.put("result", parseChildrenResults(childrenResults));

		return null;
	}

	private static List<Map<String, Object>> parseExternalReferences(List<ExternalreferenceType> externalReferences) {
		List<Map<String, Object>> list = new ArrayList<>();

		for (ExternalreferenceType ext : externalReferences) {
			Map<String, Object> el = new HashMap<>();
			String inferenceprovenance = ext.getInferenceprovenance();
			OptionalClassedSchemedElement qualifier = ext.getQualifier();
			String refidentifier = ext.getRefidentifier();
			String sitename = ext.getSitename();
			String trust = ext.getTrust();
			String url = ext.getUrl();
			Map<String, Object> qualifierMap = parseOptionalClassedSchemedElement(qualifier);

			el.put("inferenceprovenance", inferenceprovenance);
			el.put("qualifier", qualifierMap);
			el.put("refidentifier", refidentifier);
			el.put("sitename", sitename);
			el.put("trust", trust);
			el.put("url", url);

			list.add(el);

		}

		return list;
	}

	private static List<Map<String, Object>> parseInstanceTypes(List<InstanceType> instanceTypes) {
		List<Map<String, Object>> results = new ArrayList<>();
		for (InstanceType instanceType : instanceTypes) {
			Map<String, Object> obj = new HashMap<>();

			String id = instanceType.getId();
			List<JAXBElement<?>> a = instanceType.getLicenceOrInstancetypeOrHostedby();

			List<Map<String, Object>> listJAXBElement = new ArrayList<>();
			for (JAXBElement<?> af : a) {
				Map<String, Object> elementMap = extractJAXBElement(af);
				listJAXBElement.add(elementMap);
			}

			obj.put("id", id);
			obj.put("licenceOrInstancetypeOrHostedby", listJAXBElement);
			results.add(obj);
			// return listJAXBElement;

		}

		return results;
	}

	private static List<Map<String, Object>> parseChildrenResults(List<ChildrenResult> childrenResults) {
		List<Map<String, Object>> results = new ArrayList<>();
		for (ChildrenResult children : childrenResults) {
			String objectIdentifier = children.getObjidentifier();
			List<JAXBElement<?>> a = children.getTitleOrDateofacceptanceOrPublisher();

			List<Map<String, Object>> listJAXBElement = new ArrayList<>();
			for (JAXBElement<?> af : a) {
				Map<String, Object> elementMap = extractJAXBElement(af);
				listJAXBElement.add(elementMap);
			}
			Map<String, Object> obj = new HashMap<>();
			obj.put("objectIdentifier", objectIdentifier);
			obj.put("titleOrDateofacceptanceOrPublisher", listJAXBElement);

			results.add(obj);

		}

		return results;
	}

	public static Map<String, Object> parseNamedIdElement(NamedIdElement value) {

		Map<String, Object> result = new HashMap<>();
		String nameIdEl = value.getName();
		// System.out.println("\tnameIdEl: " + nameIdEl);
		result.put("name", nameIdEl);
		String inferenceProvenance = value.getInferenceprovenance();
		// System.out.println("\tinferenceProvenance: " + inferenceProvenance);
		result.put("inferenceProvenance", inferenceProvenance);
		String trust = value.getTrust();
		// System.out.println("\ttrust: " + trust);
		result.put("trust", trust);
		String id = value.getId();
		// System.out.println("\tid: " + id);
		result.put("id", id);
		return result;
	}

	public static Map<String, Object> parseOptionalClassedSchemedElement(OptionalClassedSchemedElement value) {
		// OptionalClassedSchemedElement value = (OptionalClassedSchemedElement) obj;
		Map<String, Object> result = new HashMap<>();
		String classid = value.getClassid();
		result.put("classid", classid);
		// System.out.println("\tclassid: " + classid);
		String classname = value.getClassname();
		// System.out.println("\tclassname: " + classname);
		result.put("classname", classname);
		String content = value.getContent();
		// System.out.println("\tcontent: " + content);
		result.put("content", content);
		String inferenceprovenance = value.getInferenceprovenance();
		// System.out.println("\tinferenceprovenance: " + inferenceprovenance);
		result.put("inferenceprovenance", inferenceprovenance);
		String schemeid = value.getSchemeid();
		// System.out.println("\tschemeid: " + schemeid);
		result.put("schemeid", schemeid);
		String schemename = value.getSchemename();
		// System.out.println("\tschemename: " + schemename);
		result.put("schemename", schemename);
		String trust = value.getTrust();
		// System.out.println("\ttrust: " + trust);
		result.put("trust", trust);
		return result;
	}

	public static Map<String, Object> parseClassedSchemedElement(ClassedSchemedElement value) {
		Map<String, Object> result = new HashMap<>();
		String classid = value.getClassid();
		result.put("classid", classid);
		// System.out.println("\tclassid: " + classid);
		String classname = value.getClassname();
		// System.out.println("\tclassname: " + classname);
		result.put("classname", classname);
		String content = value.getContent();
		// System.out.println("\tcontent: " + content);
		result.put("content", content);
		String inferenceprovenance = value.getInferenceprovenance();
		// System.out.println("\tinferenceprovenance: " + inferenceprovenance);
		result.put("inferenceprovenance", inferenceprovenance);
		String schemeid = value.getSchemeid();
		// System.out.println("\tschemeid: " + schemeid);
		result.put("schemeid", schemeid);
		String schemename = value.getSchemename();
		// System.out.println("\tschemename: " + schemename);
		result.put("schemename", schemename);
		String trust = value.getTrust();
		// System.out.println("\ttrust: " + trust);
		result.put("trust", trust);
		return result;
	}

	public static Map<String, Object> parseFundingFlatType(FundingFlatType fundingFlatType) {
		Map<String, Object> result = new HashMap<>();
		FunderFlatType funder = fundingFlatType.getFunder();
		String name = funder.getName();
		// System.out.println("name: " + name);
		result.put("name", name);
		String shortname = funder.getShortname();
		// System.out.println("shortname: " + shortname);
		result.put("shortname", shortname);
		String id = funder.getId();
		// System.out.println("id: " + id);
		result.put("id", id);
		String jurisdiction = funder.getJurisdiction();
		// System.out.println("jurisdition: " + jurisdition);
		result.put("jurisdiction", jurisdiction);
		String value = funder.getValue();
		// System.out.println("value: " + value);
		result.put("value", value);

		List<NamedFundingLevel> level0 = fundingFlatType.getFundingLevel0();
		Map<String, Object> levO = parseNameFundingLevel(level0);
		result.put("level0", levO);
		List<NamedFundingLevel> level1 = fundingFlatType.getFundingLevel1();
		Map<String, Object> lev1 = parseNameFundingLevel(level1);
		result.put("level1", lev1);
		List<NamedFundingLevel> level2 = fundingFlatType.getFundingLevel2();
		Map<String, Object> lev2 = parseNameFundingLevel(level2);
		result.put("level2", lev2);

		return result;

	}

	private static Map<String, Object> parseNameFundingLevel(List<NamedFundingLevel> level) {
		Map<String, Object> result = new HashMap<>();
		for (NamedFundingLevel nfl : level) {
			String name = nfl.getName();
			// System.out.println("name: " + name);
			result.put("name", name);
			String value = nfl.getValue();
			result.put("value", value);
			// System.out.println("value: " + value);
		}
		return result;
	}

	private static Map<String, Object> parseRelToType(RelToType r) {
		// RelToType r = (RelToType) element.getValue();
		Map<String, Object> result = new HashMap<>();
		String clazz = r.getClazz();
		// System.out.println("\t\t\tclazz: " + clazz);
		result.put("clazz", clazz);
		String scheme = r.getScheme();
		// System.out.println("\t\t\tscheme: " + scheme);
		result.put("scheme", scheme);
		String type = r.getType();
		// System.out.println("\t\t\ttype: " + type);
		result.put("type", type);
		String v = r.getValue();
		// System.out.println("\t\t\tv: " + v);
		result.put("value", v);
		return result;

	}

	private static List<Map<String, Object>> parseRelsType(RelsType value) {
		// RelsType value = (RelsType) obj;
		List<Map<String, Object>> mapList = new ArrayList<>();

		for (RelType rel : value.getRel()) {
			Map<String, Object> relMap = new HashMap<String, Object>();
			String inferenceprovenance = rel.getInferenceprovenance();
			String provenanceaction = rel.getProvenanceaction();
			String trust = rel.getTrust();
			// System.out.println("-----------------------------------------------");
			// System.out.println("\tREL:");
			// System.out.println("\tinferenceprovenance: " + inferenceprovenance);
			// System.out.println("\tprovenanceaction: " + provenanceaction);
			// System.out.println("\ttrust: " + trust);

			relMap.put("inferenceprovenance", inferenceprovenance);
			relMap.put("provenanceaction", provenanceaction);
			relMap.put("trust", trust);

			// List<Map<String, Object>> listElement = new ArrayList<>();

			for (JAXBElement<?> relElement : rel.getToOrTitleOrWebsiteurl()) {
				// decide what to todo
				// System.out.println("\t\tname: " + relElement.getName().toString() + " value:
				// "
				// + relElement.getValue().toString());
				// System.out.println("\t\tname: " + relElement.getName().toString());
				String NAME = relElement.getName().toString();

				if (relElement.getValue() instanceof RelToType) {
					RelToType r = (RelToType) relElement.getValue();
					Map<String, Object> m = parseRelToType(r);
					// Map<String, Object> res = new HashMap<>();
					relMap.put(NAME, m);
					// listElement.add(res);

				} else if (relElement.getValue() instanceof OptionalClassedSchemedElement) {
					OptionalClassedSchemedElement val = (OptionalClassedSchemedElement) relElement.getValue();
					Map<String, Object> o = parseOptionalClassedSchemedElement(val);
					// Map<String, Object> res = new HashMap<>();
					relMap.put(NAME, o);
					// listElement.add(res);

				} else if (relElement.getValue() instanceof FundingFlatType) {
					FundingFlatType fund = (FundingFlatType) relElement.getValue();
					Map<String, Object> o = parseFundingFlatType(fund);
					// Map<String, Object> res = new HashMap<>();
					relMap.put(NAME, o);
					// listElement.add(res);

				} else if (relElement.getValue() instanceof String) {
					String val = (String) relElement.getValue();
					// Map<String, Object> res = new HashMap<>();
					relMap.put(NAME, val);

				} else if (relElement.getValue() instanceof WebresourceType) {
					// String val = (String) relElement.getValue();

					WebresourceType web = (WebresourceType) relElement.getValue();
					String url = web.getUrl();
					// Map<String, Object> res = new HashMap<>();
					relMap.put(NAME, url);

				} else if (relElement.getValue() instanceof ClassedSchemedElement) {
					ClassedSchemedElement cl = (ClassedSchemedElement) relElement.getValue();
					Map<String, Object> o = parseClassedSchemedElement(cl);
					// Map<String, Object> res = new HashMap<>();
					relMap.put(NAME, o);
					// listElement.add(res);

				} else {
					// Map<String, Object> res = new HashMap<>();
					relMap.put(NAME, null);
					// listElement.add(res);

				}

				// relMap.put("ToOrTitleOrWebsiteurl", listElement);

				// System.out.println("\t" + relElement.getValue().toString());
			}
			mapList.add(relMap);

		}
		return mapList;
	}

	// private static List<Map<String, Object>> parseRelsType(RelsType value) {
	// // RelsType value = (RelsType) obj;
	// List<Map<String, Object>> mapList = new ArrayList<>();
	//
	// for (RelType rel : value.getRel()) {
	// Map<String, Object> relMap = new HashMap<String, Object>();
	// String inferenceprovenance = rel.getInferenceprovenance();
	// String provenanceaction = rel.getProvenanceaction();
	// String trust = rel.getTrust();
	// // System.out.println("-----------------------------------------------");
	// // System.out.println("\tREL:");
	// // System.out.println("\tinferenceprovenance: " + inferenceprovenance);
	// // System.out.println("\tprovenanceaction: " + provenanceaction);
	// // System.out.println("\ttrust: " + trust);
	//
	// relMap.put("inferenceprovenance", inferenceprovenance);
	// relMap.put("provenanceaction", provenanceaction);
	// relMap.put("trust", trust);
	//
	// List<Map<String, Object>> listElement = new ArrayList<>();
	//
	// for (JAXBElement<?> relElement : rel.getToOrTitleOrWebsiteurl()) {
	// // decide what to todo
	// // System.out.println("\t\tname: " + relElement.getName().toString() + "
	// value:
	// // "
	// // + relElement.getValue().toString());
	// // System.out.println("\t\tname: " + relElement.getName().toString());
	// String NAME = relElement.getName().toString();
	//
	// if (relElement.getValue() instanceof RelToType) {
	// RelToType r = (RelToType) relElement.getValue();
	// Map<String, Object> m = parseRelToType(r);
	// Map<String, Object> res = new HashMap<>();
	// res.put(NAME, m);
	// listElement.add(res);
	//
	// } else if (relElement.getValue() instanceof OptionalClassedSchemedElement) {
	// OptionalClassedSchemedElement val = (OptionalClassedSchemedElement)
	// relElement.getValue();
	// Map<String, Object> o = parseOptionalClassedSchemedElement(val);
	// Map<String, Object> res = new HashMap<>();
	// res.put(NAME, o);
	// listElement.add(res);
	//
	// } else if (relElement.getValue() instanceof FundingFlatType) {
	// FundingFlatType fund = (FundingFlatType) relElement.getValue();
	// Map<String, Object> o = parseFundingFlatType(fund);
	// Map<String, Object> res = new HashMap<>();
	// res.put(NAME, o);
	// listElement.add(res);
	//
	// } else if (relElement.getValue() instanceof String) {
	// String val = (String) relElement.getValue();
	// Map<String, Object> res = new HashMap<>();
	// res.put(NAME, val);
	//
	// } else if (relElement.getValue() instanceof WebresourceType) {
	// // String val = (String) relElement.getValue();
	//
	// WebresourceType web = (WebresourceType) relElement.getValue();
	// String url = web.getUrl();
	// Map<String, Object> res = new HashMap<>();
	// res.put(NAME, url);
	//
	// } else if (relElement.getValue() instanceof ClassedSchemedElement) {
	// ClassedSchemedElement cl = (ClassedSchemedElement) relElement.getValue();
	// Map<String, Object> o = parseClassedSchemedElement(cl);
	// Map<String, Object> res = new HashMap<>();
	// res.put(NAME, o);
	// listElement.add(res);
	//
	// } else {
	// Map<String, Object> res = new HashMap<>();
	// res.put(NAME, null);
	// listElement.add(res);
	// //
	// System.err.println("----------------------------------------------------------");
	// // System.err.println("Not parsable value: " +
	// // relElement.getValue().toString());
	// // System.err.println("Not parsable value: " +
	// // relElement.getValue().getClass());
	// // System.err.println("Not parsable value: " + relElement.getValue());
	// }
	//
	// relMap.put("ToOrTitleOrWebsiteurl", listElement);
	// mapList.add(relMap);
	//
	// // System.out.println("\t" + relElement.getValue().toString());
	// }
	//
	// }
	// return mapList;
	// }

}
