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

import javax.swing.text.Utilities;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.PropertyException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.lang3.builder.RecursiveToStringStyle;
import org.openarchives.oai._2.HeaderType;
import org.openarchives.oai._2.ListRecordsType;
import org.openarchives.oai._2.OAIPMHtype;
import org.openarchives.oai._2.RecordType;

import eu.openaire.namespace.oaf.ChildrenOrg;
import eu.openaire.namespace.oaf.ClassedSchemedElement;
import eu.openaire.namespace.oaf.DatainfoType;
import eu.openaire.namespace.oaf.Entity;
import eu.openaire.namespace.oaf.FunderFlatType;
import eu.openaire.namespace.oaf.FunderType;
import eu.openaire.namespace.oaf.FundingFlatType;
import eu.openaire.namespace.oaf.FundingParentType;
import eu.openaire.namespace.oaf.FundingTreeType;
import eu.openaire.namespace.oaf.FundingType;
import eu.openaire.namespace.oaf.NamedFundingLevel;
import eu.openaire.namespace.oaf.NamedIdElement;
import eu.openaire.namespace.oaf.OptionalClassedSchemedElement;
import eu.openaire.namespace.oaf.Project.Children;
import eu.openaire.namespace.oaf.RelToType;
import eu.openaire.namespace.oaf.RelType;
import eu.openaire.namespace.oaf.RelsType;
import eu.openaire.namespace.oaf.WebresourceType;

public class ParserProject {

	public static void main(String[] args) {
		List<Map<String, Object>> projects = retriveProjectFromXmlFile(
				"/Users/paolosottovia/eclipse-workspace/eusalp/output/projects_1.xml");
		System.out.println("Number of organizations: " + projects.size());

		Utils.printStructure(projects.get(0), 0);

		for (Map<String, Object> pr : projects) {
			Utils.printStructure(pr, 0);

			System.out.println("\n\n======================================\n\n");

		}
	}

	public static List<Map<String, Object>> retrieveProjectFromOpenAireIds(String folderPath, String prefix,
			Set<String> ids) {

		List<String> paths = Utils.retrievePaths(folderPath, prefix);
		List<Map<String, Object>> results = new ArrayList<>();

		for (String path : paths) {
			results.addAll(filterProjectsById(retriveProjectFromXmlFile(path), ids));
		}

		return results;
	}

	private static List<Map<String, Object>> filterProjectsById(List<Map<String, Object>> projects, Set<String> ids) {
		List<Map<String, Object>> results = new ArrayList<>();
		for (Map<String, Object> project : projects) {
			String openAireId = (String) project.get("openaire_identifier");
			openAireId = openAireId.replace("oai:dnet:", "");

			// for (String id : ids) {
			//
			// if(openAireId.contains(id)){
			// project.put("openaire_identifier_mod", openAireId);
			// results.add(project);
			// break;
			// }
			//
			//
			// }

			if (ids.contains(openAireId)) {

				project.put("openaire_identifier_mod", openAireId);
				results.add(project);
			}
		}

		return results;
	}

	private static List<Map<String, Object>> retriveProjectFromXmlFile(String path) {
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
				eu.openaire.namespace.oaf.Project org = ent.getProject();

				List<Map<String, Object>> listJAXBElement = parseProject(org);

				String identifier = header.getIdentifier();
				// System.out.println("identifier: " + identifier);
				Map<String, Object> organization = compressProject(listJAXBElement);
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

	public static List<Map<String, Object>> parseProject(eu.openaire.namespace.oaf.Project project) {

		List<JAXBElement<?>> a = project.getCodeOrAcronymOrTitle();
		List<Map<String, Object>> listJAXBElement = new ArrayList<>();
		for (JAXBElement<?> af : a) {
			Map<String, Object> elementMap = extractJAXBElement(af);
			listJAXBElement.add(elementMap);
		}

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

			// } else if (obj instanceof ChildrenOrg) {
			// ChildrenOrg value = (ChildrenOrg) obj;
			// List<eu.openaire.namespace.oaf.ChildrenOrg.Organization> orgs =
			// value.getOrganization();
			// // System.out.println("\t Number of children organizations: " + orgs.size());
			// List<Map<String, Object>> orgsList = new ArrayList<>();
			// for (eu.openaire.namespace.oaf.ChildrenOrg.Organization org : orgs) {
			// //
			// // todo parse orgs recursevely
			// List<Map<String, Object>> orgsMap = parseChildrenOrganization(org);
			// Map<String, Object> orgsMapCompress = compressOrganization(orgsMap);
			// orgsList.add(orgsMapCompress);
			// }
			// result.put(name, orgsList);

		} else if (obj instanceof NamedIdElement) {
			NamedIdElement value = (NamedIdElement) obj;
			Map<String, Object> nameIdElementMap = parseNamedIdElement(value);
			result.put(name, nameIdElementMap);
		} else if (obj instanceof ClassedSchemedElement) {
			ClassedSchemedElement value = (ClassedSchemedElement) obj;
			Map<String, Object> resClassedSchemedElement = parseClassedSchemedElement(value);
			result.put(name, resClassedSchemedElement);

		} else if (obj instanceof Boolean) {
			Boolean value = (Boolean) obj;
		} else if (obj instanceof FundingTreeType) {
			FundingTreeType value = (FundingTreeType) obj;
			Map<String, Object> res = parseFundingTreeType(value);
			result.put(name, res);
		}

		else if (obj instanceof Children) {

			Children value = (Children) obj;
			result.put(name, value.getContent());
		}

		else {
			System.err.println("OTHER TYPE: " + obj.getClass().getName());
			result.put(name, null);
		}

		return result;

	}

	private static Map<String, Object> parseFundingTreeType(FundingTreeType value) {
		Map<String, Object> result = new HashMap<>();

		FunderType funderType = value.getFunder();
		result.put("funderType", parseFunderType(funderType));
		List<JAXBElement<FundingType>> a = value.getFundingLevel2OrFundingLevel1OrFundingLevel0();

		List<Map<String, Object>> fundingTypes = new ArrayList<>();
		for (JAXBElement<FundingType> el : a) {
			fundingTypes.add(parseFundingType(el.getValue()));
		}
		result.put("fundingLevel2OrFundingLevel1OrFundingLevel0", fundingTypes);

		return result;
	}

	private static Map<String, Object> parseFundingType(FundingType fundingType) {
		Map<String, Object> result = new HashMap<>();
		if (fundingType == null) {
			return null;
		}

		String clazz = fundingType.getClazz();
		result.put("clazz", clazz);
		String description = fundingType.getDescription();
		result.put("description", description);
		String id = fundingType.getId();
		result.put("id", id);
		String name = fundingType.getName();
		result.put("name", name);

		FundingParentType parent = fundingType.getParent();
		Map<String, Object> parentMap = new HashMap<>();
		Map<String, Object> fundingLevel0 = parseFundingType(parent.getFundingLevel0());
		parentMap.put("fundingLevel0", fundingLevel0);
		Map<String, Object> fundingLevel1 = parseFundingType(parent.getFundingLevel1());
		parentMap.put("fundingLevel1", fundingLevel1);
		result.put("parent", parentMap);

		return result;

	}

	private static Map<String, Object> parseFunderType(FunderType funderType) {
		Map<String, Object> results = new HashMap<>();

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

	public static Map<String, Object> compressProject(List<Map<String, Object>> organization) {
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

		}
		return map;
	}

}
