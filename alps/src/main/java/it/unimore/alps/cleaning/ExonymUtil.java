package it.unimore.alps.cleaning;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExonymUtil {

	public Map<String, String> retrieveDictionary(List<Exonym> exonyms, List<String> places, String inputLanguage,
			String outputLanguage) {

		Map<Integer, List<Exonym>> map = groupbyGeonameId(exonyms);

		String inputL = inputLanguage.toLowerCase().trim();
		String outputL = outputLanguage.toLowerCase().trim();

		Map<String, String> dictionary = new HashMap<>();

		for (Map.Entry<Integer, List<Exonym>> entry : map.entrySet()) {

			List<List<String>> results = getCandidates(entry.getValue(), places, inputL, outputL);

			for (List<String> res : results) {
				if (res != null) {

					String key = res.get(0);
					String value = res.get(1);

					if (dictionary.containsKey(key)) {

						if (!value.equals(dictionary.get(key))) {

							System.err.println("duplicate key: " + key + " actual result: " + value + " old value: "
									+ dictionary.get(key));
						}
					} else {
						dictionary.put(key, value);
					}

				}

			}

		}

		return dictionary;

	}

	public List<String> retrievePlacesToLoweCase(Collection<String> places) {
		List<String> placesLowerCase = new ArrayList<>();

		for (String place : places) {
			placesLowerCase.add(place.toLowerCase().trim());
		}

		return placesLowerCase;

	}

	private List<List<String>> getCandidates(List<Exonym> exonyms, List<String> places, String languageSource,
			String languageDestination) {

		List<List<String>> results = new ArrayList<>();
		String source = null;
		String destination = null;

		boolean findUsual = false;
		boolean matchPlacesPostalCodeDestination = false;
		boolean matchPlacesPostalCodeSource = false;
		for (Exonym ex : exonyms) {

			if (ex.getIsolanguage() != null) {

				if (ex.getIsolanguage().equals(languageSource)) {
					if (!matchPlacesPostalCodeSource) {
						source = ex.getAlternateName().toLowerCase().trim();
						if (places.contains(source)) {
							matchPlacesPostalCodeSource = true;
						}
					}
				}
				if (ex.getIsolanguage().equals(languageDestination)) {
					if (!matchPlacesPostalCodeDestination) {
						if (findUsual == false) {
							destination = ex.getAlternateName();
						}

						if (ex.getIsPreferredName() != null) {
							if (ex.getIsPreferredName().intValue() == 1) {
								destination = ex.getAlternateName();
								findUsual = true;
							}
						}

						if (places.contains(ex.getAlternateName().toLowerCase().trim())) {
							matchPlacesPostalCodeDestination = true;
							destination = ex.getAlternateName().trim();
							System.out.println("MATCH: " + ex.getAlternateName().toLowerCase().trim());
						}
					}
				}
			}

		}

		if (source != null && destination != null)

		{
			List<String> result = new ArrayList<>();
			result.add(source);
			result.add(destination);
			results.add(result);

		} else {

			// System.out.println("--------------------------------------------");

			for (Exonym ex : exonyms) {
				if (ex.isolanguage == null) {
					List<String> result = new ArrayList<>();
					result.add(ex.getAlternateName().toLowerCase());
					result.add(ex.getAlternateName());
					results.add(result);
				}
			}

		}

		return results;

	}

	public Map<Integer, List<Exonym>> groupbyGeonameId(List<Exonym> exonomys) {

		Map<Integer, List<Exonym>> idExonyms = new HashMap();

		for (Exonym e : exonomys) {

			Integer id = e.getGeonameid();

			if (idExonyms.containsKey(id)) {
				idExonyms.get(id).add(e);
			} else {
				List<Exonym> exs = new ArrayList<>();
				exs.add(e);
				idExonyms.put(id, exs);
			}

		}

		return idExonyms;

	}

	public List<Exonym> extractExonym(String file, String country) {
		List<Exonym> exonyms = new ArrayList<>();

		try {
			File fileDir = new File(file);

			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(fileDir), "UTF8"));

			String str;

			while ((str = in.readLine()) != null) {
				// System.out.println(str);

				if (!str.trim().equals("")) {

					Exonym ex = transformLineIntoExonym(str, country);
					exonyms.add(ex);
				}

			}

			in.close();
		} catch (UnsupportedEncodingException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}

		return exonyms;
	}

	private Exonym transformLineIntoExonym(String line, String country) {

		String[] elements = line.split("\t", -1);

		Integer alternateNameId = parseInteger(elements[0]);
		Integer geonameid = parseInteger(elements[1]);
		String isolanguage = parseString(elements[2]);
		String alternateName = parseString(elements[3]);
		Integer isPreferredName = parseInteger(elements[4]);
		Integer isShortName = parseInteger(elements[5]);
		Integer isColloquial = parseInteger(elements[6]);
		Integer isHistoric = parseInteger(elements[7]);
		String from = null;
		String to = null;

		return new Exonym(alternateNameId, geonameid, isolanguage, alternateName, isPreferredName, isShortName,
				isColloquial, isHistoric, from, to, country);

	}

	private String parseString(String input) {
		if (input.trim().equals("")) {
			return null;
		} else {
			return input.trim();
		}
	}

	private Integer parseInteger(String input) {

		if (!input.trim().equals("")) {

			return Integer.parseInt(input.trim());

		} else {
			return null;
		}

	}

	public static void main(String[] args) {

		// System.out.println("START ");
		// ExonymUtil util = new ExonymUtil();
		//
		// List<Exonym> exonyms =
		// util.extractExonym("/Users/paolosottovia/Downloads/Alternatives/IT/IT.txt",
		// "IT");
		// System.out.println("Number of exonyms: " + exonyms.size());
		//
		//
		//
		//
		//
		//
		//
		// Map<String, String> dictionary = util.retrieveDictionary(exonyms, "en",
		// "it");
		// System.out.println("Dictionary size: " + dictionary.size());
		//
		// if (dictionary.containsKey("rome")) {
		// System.out.println("IT rome: " + dictionary.get("rome"));
		// }
		//
		// Map<String, String> dictionaryIT = util.retrieveDictionary(exonyms, "it",
		// "it");
		//
		// System.out.println("Dictionary IT-IT size: " + dictionaryIT.size());
		// if (dictionaryIT.containsKey("roma")) {
		// System.out.println("IT roma: " + dictionaryIT.get("roma"));
		// }

	}
}
