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

import eu.openaire.namespace.oaf.ChildrenOrg;
import eu.openaire.namespace.oaf.ClassedSchemedElement;
import eu.openaire.namespace.oaf.DatainfoType;
import eu.openaire.namespace.oaf.Entity;
import eu.openaire.namespace.oaf.FunderFlatType;
import eu.openaire.namespace.oaf.FundingFlatType;
import eu.openaire.namespace.oaf.NamedFundingLevel;
import eu.openaire.namespace.oaf.NamedIdElement;
import eu.openaire.namespace.oaf.OptionalClassedSchemedElement;
import eu.openaire.namespace.oaf.RelToType;
import eu.openaire.namespace.oaf.RelType;
import eu.openaire.namespace.oaf.RelsType;
import eu.openaire.namespace.oaf.WebresourceType;
import it.unimore.alps.sources.cordis.CordisOrganization;
import it.unimore.alps.sql.model.ExternalParticipant;

public class ParserOrganization {

	public static void main(String[] args) {

		// List<Map<String, Object>> orgs =
		// retriveOrganizationFromXmlFile("output/out_1.xml");
		// System.out.println("Number of organizations: " + orgs.size());
		//
		// Set<String> countries = new HashSet<>();
		// countries.add("IT");
		//
		// filterOrganizationsByCountry("", countries);
		//
		// visit(orgs.get(0), 0);

		List<Map<String, Object>> italianOrganizations = getOrganizationsFromFilesByCountry(
				"/Users/paolosottovia/eclipse-workspace/eusalp/output/", "out_", "IT");
		//
		// System.out.println("Number of italian organizations: " +
		// italianOrganizations.size());
		//
		// visit(italianOrganizations.get(0), 0);
		//
		// Set<String> projectsIds = getProjectsFromOrganizations(italianOrganizations);
		//
		// for (String projectId : projectsIds) {
		// System.out.println("Project ID: " + projectId);
		// }

		Set<String> names = new HashSet<>();
		// names.add("SOCIETE MARINE DE SERVICE ET D'EQUIPEMENT NAVAL");
		// names.add("RESEAU POUR LA SECURITE INTEGREE ASSOCIATION");
		// names.add("HONEYWELL AEROSPACE");
		// names.add("French Institute of bioinformatics");

		names.add("Enel S.p.A.");

		List<Map<String, Object>> filteredOrganizations = getOrganizationsFromFilesByName(
				"/Users/paolosottovia/eclipse-workspace/eusalp/output/", "out_", names);

		for (Map<String, Object> org : filteredOrganizations) {
			System.out.println("------------------------------------------------------------------------------------");
			visit(org, 0);
		}

		System.out.println("Number of companies: " + filteredOrganizations.size());

		// Map<String, List<Map<String, Object>>> map =
		// getOrganizationsFromFilesByOriginalID(
		// "/Users/paolosottovia/eclipse-workspace/eusalp/output/", "out_");
		Map<String, List<Map<String, Object>>> map = filterGivenOrganizationsByOriginalId(italianOrganizations);
		System.out.println("Number of companies: " + italianOrganizations.size());
		System.out.println("Number of organizations: " + map.size());

		System.out.println("Enel: " + map.get("::999804318").size());

		int max = -1;

		for (Map.Entry<String, List<Map<String, Object>>> entry : map.entrySet()) {
			if (entry.getValue().size() > max) {
				max = entry.getValue().size();
			}
		}
		System.out.println("MAX: " + max);
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

	public static List<Map<String, Object>> getOrganizationsFromFilesByCountry(String inputFolder, String prefix,
			String country) {

		List<Map<String, Object>> results = new ArrayList<>();
		Set<String> countries = new HashSet<>();

		if (country.contains(",")) {
			String[] c = country.split(",");
			for (String cou : c) {
				countries.add(cou);
			}
			System.out.println("Countries: " + countries.toString());
		} else {
			countries.add(country);
		}
		List<String> files = Utils.retrievePaths(inputFolder, prefix);
		for (String file : files) {
			results.addAll(filterOrganizationsByCountry(file, countries));

		}

		return results;
	}

	private static String extractCordisId(Map<String, Object> organization) {
		if ((organization.get("originalId")) instanceof String) {
			String id = ((String) organization.get("originalId"));
			if (id.contains("::")) {
				System.out.println("\t\tOriginalID: ");
				id = id.substring(id.indexOf("::") + 2);
			}
			System.out.println("ID: " + id);

			return id;

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

					return id;

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

							return id;

						}

					} else {
						System.err.println("Original ID: " + map.toString());
					}

				} else {
					System.err.println("Original ID: " + map.toString());
				}

			}

		}
		return null;
	}

	public static Map<String, ExternalParticipant> collectExtraOrganizations(String inputFolder, String prefix,
			List<Map<String, Object>> projects, String country,
			Map<String, CordisOrganization> codeCordisOrganizations) {
		Set<String> countries = new HashSet<>();
		System.out.println("\nEXTRA ORGANIZATION COLLECTION");
		if (country.contains(",")) {
			String[] c = country.split(",");
			for (String cou : c) {
				countries.add(cou);
			}
			System.out.println("Countries: " + countries.toString());
		} else {
			countries.add(country);
		}

		Set<String> openAireIds = new HashSet<>();

		for (Map<String, Object> project : projects) {

			if (project.containsKey("rels")) {

				List<Map<String, Object>> rels = (List<Map<String, Object>>) project.get("rels");

				for (Map<String, Object> rel : rels) {

					if (rel.containsKey("country")) {

						Map<String, Object> countryMap = (Map<String, Object>) rel.get("country");

						String id = (String) countryMap.get("classid");
						if (!countries.contains(id)) {
							// add extra organization

							if (rel.containsKey("to")) {

								Map<String, Object> to = (Map<String, Object>) rel.get("to");

								if (to.containsKey("clazz")) {
									String clazz = (String) to.get("clazz");
									if (clazz.equals("hasParticipant")) {

										String value_id = (String) to.get("value");
										openAireIds.add(value_id);

									}
								}

							}

						}

					} else {
						System.err.println("ERROR: empty country!!!");

						visit(rel, 0);
						System.err.println("\n\n");

					}

				}

			}

		}

		System.out.println("Number of openaire IDS: " + openAireIds.size());

		for (String id : openAireIds) {
			System.out.print(id + "\t");
		}
		System.out.println("\n\n\n");

		Map<String, Map<String, Object>> organizations = getOrganizationsFromFilesByOpenAireIds(inputFolder, prefix,
				openAireIds);

		System.out.println("Number of organizations: " + organizations.size());

		Map<String, ExternalParticipant> openAireID_ExternalParticipants = new HashMap<>();

		for (Map.Entry<String, Map<String, Object>> org : organizations.entrySet()) {

			Map<String, Object> organization = org.getValue();

			String code = extractCordisId(organization);

			if (code != null) {
				// String code = (String) organization.get("code");

				System.out.println("Code: " + code);

				if (codeCordisOrganizations.containsKey(code)) {
					CordisOrganization cordisOrganization = codeCordisOrganizations.get(code);
					String label = cordisOrganization.getName();
					String url = cordisOrganization.getOrganizationUrl();

					ExternalParticipant exParticipant = new ExternalParticipant();
					exParticipant.setLabel(label);
					exParticipant.setUrl(url);

					openAireID_ExternalParticipants.put(org.getKey(), exParticipant);
				}

			}

		}

		return openAireID_ExternalParticipants;

	}

	public static Map<String, Map<String, Object>> getOrganizationsFromFilesByOpenAireIds(String inputFolder,
			String prefix, Set<String> openAireIds) {

		Map<String, Map<String, Object>> results = new HashMap<>();
		Set<String> countries = new HashSet<>();

		List<String> files = Utils.retrievePaths(inputFolder, prefix);
		for (String file : files) {

			for (Map.Entry<String, Map<String, Object>> item : filterOrganizationsByOpenAireIds(file, openAireIds)
					.entrySet()) {

				if (results.containsKey(item.getKey())) {
					System.err.println("Duplicate id found");
					System.exit(0);
				} else {
					results.put(item.getKey(), item.getValue());
				}

			}

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

	public static List<Map<String, Object>> filterOrganizationsByCountry(String path, Set<String> countryIds) {

		List<Map<String, Object>> filesOrganizations = retriveOrganizationFromXmlFile(path);
		List<Map<String, Object>> organizationsFiltered = new ArrayList<>();

		for (Map<String, Object> organization : filesOrganizations) {

			Map<String, Object> country = (Map<String, Object>) organization.get("country");

			// System.out.println(country.keySet().toString());

			String id = (String) country.get("classid");
			if (countryIds.contains(id)) {
				organizationsFiltered.add(organization);
			}

		}

		return organizationsFiltered;

	}

	public static Map<String, Map<String, Object>> filterOrganizationsByOpenAireIds(String path,
			Set<String> openAireIds) {

		List<Map<String, Object>> filesOrganizations = retriveOrganizationFromXmlFile(path);
		Map<String, Map<String, Object>> organizationsFiltered = new HashMap<>();

		for (Map<String, Object> organization : filesOrganizations) {

			if (organization.containsKey("openaire_identifier")) {

				String openaire_identifier = (String) organization.get("openaire_identifier");

				// System.out.println(country.keySet().toString());

				for (String id : openAireIds) {
					if (openaire_identifier.contains(id)) {
						if (organizationsFiltered.containsKey(id)) {
							System.err.println("Error duplicate id!!!!!");
							System.exit(0);
						} else {
							organizationsFiltered.put(id, organization);
						}
					}
				}

			}

		}

		return organizationsFiltered;

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
				eu.openaire.namespace.oaf.Organization org = ent.getOrganization();
				List<Map<String, Object>> listJAXBElement = parseOrganization(org);

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

	public static List<Map<String, Object>> parseOrganization(eu.openaire.namespace.oaf.Organization organization) {

		List<JAXBElement<?>> a = organization.getLegalnameOrLegalshortnameOrLogourl();
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
		}

		else {
			// System.out.println("OTHER TYPE: " + obj.getClass().getName());
			result.put(name, null);
		}

		return result;

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
