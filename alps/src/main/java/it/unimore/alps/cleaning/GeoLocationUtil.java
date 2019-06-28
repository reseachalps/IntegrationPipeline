package it.unimore.alps.cleaning;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import it.unimore.alps.sql.model.Organization;

public class GeoLocationUtil {

	// Map<String, String> dictionaryEN_IT;
	//
	// Map<String, String> dictionaryIT_IT;
	//
	// Map<String, List<PostalCodeItem>> cityPostalCodesIT;

	Map<String, LanguageData> languageData = new HashMap<>();

	private Map<String, List<String>> loadPaths() {
		String IT_Cities = "/home/paolos/Alternatives/IT/IT.txt";
		String IT_Provinces = "/home/paolos/ZipCodes/IT/IT.txt";

		String FR_Cities = "/home/paolos/Alternatives/FR/FR.txt";
		String FR_Provinces = "/home/paolos/ZipCodes/FR/FR.txt";

		String DE_Cities = "/home/paolos/Alternatives/DE/DE.txt";
		String DE_Provinces = "/home/paolos/ZipCodes/DE/DE.txt";

		String CH_Cities = "/home/paolos/Alternatives/CH/CH.txt";
		String CH_Provinces = "/home/paolos/ZipCodes/CH/CH.txt";

		String AT_Cities = "/home/paolos/Alternatives/AT/AT.txt";
		String AT_Provinces = "/home/paolos/ZipCodes/AT/AT.txt";

		String SI_Cities = "/home/paolos/Alternatives/SI/SI.txt";
		String SI_Provinces = "/home/paolos/ZipCodes/SI/SI.txt";

		String LI_Cities = "/home/paolos/Alternatives/LI/LI.txt";
		String LI_Provinces = "/home/paolos/ZipCodes/LI/LI.txt";

		Map<String, List<String>> languagePaths = new HashMap<>();
		List<String> italy = new ArrayList<>();
		italy.add(IT_Cities);
		italy.add(IT_Provinces);
		List<String> france = new ArrayList<>();
		france.add(FR_Cities);
		france.add(FR_Provinces);

		List<String> switzerland = new ArrayList<>();
		switzerland.add(CH_Cities);
		switzerland.add(CH_Provinces);

		List<String> germany = new ArrayList<>();
		germany.add(DE_Cities);
		germany.add(DE_Provinces);

		List<String> austria = new ArrayList<>();
		austria.add(AT_Cities);
		austria.add(AT_Provinces);

		List<String> slovenia = new ArrayList<>();
		slovenia.add(SI_Cities);
		slovenia.add(SI_Provinces);

		List<String> lichtenstein = new ArrayList<>();
		lichtenstein.add(LI_Cities);
		lichtenstein.add(LI_Provinces);

		languagePaths.put("it", italy);
		languagePaths.put("fr", france);
		languagePaths.put("ch", switzerland);
		languagePaths.put("de", germany);
		languagePaths.put("at", austria);
		languagePaths.put("si", slovenia);
		languagePaths.put("li", lichtenstein);

		return languagePaths;

	}

	public void loadDatabases() {

		Map<String, List<String>> paths = loadPaths();

		// Map<String, LanguageData> languageData = new HashMap<>();

		ExonymUtil util = new ExonymUtil();

		for (Map.Entry<String, List<String>> entry : paths.entrySet()) {

			String languageCode = entry.getKey();
			List<String> files = entry.getValue();

			List<Exonym> exonyms = util.extractExonym(files.get(0), languageCode);
			System.out.println("Number of exonyms for " + languageCode + " : " + exonyms.size());

			String languageCodeUpdated = null;

			switch (languageCode) {
			case "at":
				languageCodeUpdated = "de";
				break;

			case "ch":
				languageCodeUpdated = "en";
				break;

			case "li":
				languageCodeUpdated = "";
				break;

			default:
				languageCodeUpdated = languageCode;
				break;
			}

			PostalCodeItemUtil postalCodeUtil = new PostalCodeItemUtil();
			List<PostalCodeItem> postalCodes = postalCodeUtil.extractPostalCodes(files.get(1));
			Map<String, List<PostalCodeItem>> cityPostalCodesL = postalCodeUtil.retrieveMap(postalCodes);

			List<String> places = util.retrievePlacesToLoweCase(cityPostalCodesL.keySet());
			//
			//
			//

			Map<String, String> dictionaryEN_L = util.retrieveDictionary(exonyms, places, "en", languageCodeUpdated);
			System.out.println("Dictionary EN -> " + languageCodeUpdated + " ( " + languageCode + " )" + " size: "
					+ dictionaryEN_L.size());

			// if (dictionary.containsKey("rome")) {
			// System.out.println("IT rome: " + dictionary.get("rome"));
			// }

			Map<String, String> dictionaryL_L = util.retrieveDictionary(exonyms, places, languageCodeUpdated,
					languageCodeUpdated);

			System.out.println("Dictionary " + languageCodeUpdated + " ( " + languageCode + " )" + " -> "
					+ languageCodeUpdated + " ( " + languageCode + " )" + " size: " + dictionaryL_L.size());
			// if (dictionaryIT.containsKey("roma")) {
			// System.out.println("IT roma: " + dictionaryIT.get("roma"));
			// }

			LanguageData data = new LanguageData(dictionaryEN_L, dictionaryL_L, cityPostalCodesL);

			languageData.put(languageCode, data);

		}

		// return languageData;

	}

	public LocationData checkOrganizationLocationByProvince(Organization org, String province) {

		String countryCode = org.getCountryCode().toLowerCase();
		System.out.println("-----------------------------------------------------------------------------------");
		System.out.println("COUNTRY CODE: " + countryCode);

		LocationData result = null;
		if (languageData.containsKey(countryCode)) {

			LanguageData data = languageData.get(countryCode);

			Map<String, String> dictionary = data.getDictionaryEN_L();
			Map<String, String> dictionaryL = data.getDictionaryL_L();

			Map<String, List<PostalCodeItem>> cityPostalCodes = data.getCityPostalCodesL();

			for (Map.Entry<String, List<PostalCodeItem>> i : cityPostalCodes.entrySet()) {
				for(PostalCodeItem postalCodeItem : i.getValue()) {
					String ub = postalCodeItem.getAdminCode2().toLowerCase().trim();
					
					if(ub.equals(province.toLowerCase())) {
						return new LocationData(null, postalCodeItem.getAdminName2(), postalCodeItem.getAdminCode2());
					}
						
					}
				}
			}

			

		return null;

	}

	public LocationData checkOrganizationLocation(Organization org) {

		String countryCode = org.getCountryCode().toLowerCase();
		System.out.println("-----------------------------------------------------------------------------------");
		System.out.println("COUNTRY CODE: " + countryCode);

		LocationData result = null;
		if (languageData.containsKey(countryCode)) {

			LanguageData data = languageData.get(countryCode);

			Map<String, String> dictionary = data.getDictionaryEN_L();
			Map<String, String> dictionaryL = data.getDictionaryL_L();

			Map<String, List<PostalCodeItem>> cityPostalCodes = data.getCityPostalCodesL();

			boolean p = false;
			if (org.getCity() == null) {
				return null;
			}

			String city = org.getCity().toLowerCase().replaceAll("\\(.*\\)", "").trim();

			String CITY = null;
			System.out.println("Original city: " + city);
			if (dictionary.containsKey(city)) {
				System.out.println("City: " + dictionary.get(city));

				// present++;
				p = true;
				CITY = dictionary.get(city);
			} else

			if (dictionaryL.containsKey(city)) {
				System.out.println("City: " + dictionaryL.get(city));
				// present++;
				p = true;
				CITY = dictionaryL.get(city);
			} else {
				System.out.println("City: " + org.getCity() + " is not present!");
			}

			if (p == true) {
				// if (org.getUrbanUnit() == null) {
				// System.out.println("\tNULL URBAN UNIT");

				if (cityPostalCodes.containsKey(CITY.toLowerCase())) {

					List<PostalCodeItem> psList = cityPostalCodes.get(CITY.toLowerCase());
					System.out.println("\tNumber of items: " + psList.size());

					PostalCodeItem postalCode = psList.get(0);
					String province = postalCode.getAdminName2();
					String provinceCode = postalCode.getAdminCode2();

					result = new LocationData(CITY, province, provinceCode);

					System.out.println("\tProvince: " + province + "\tProvince code: " + provinceCode);

				} else {
					System.out.println("\tProvince not found!");
				}

				// }
			} else {

				if (cityPostalCodes.containsKey(city.toLowerCase())) {

					List<PostalCodeItem> psList = cityPostalCodes.get(city.toLowerCase());
					System.out.println("\tNumber of items: " + psList.size());

					PostalCodeItem postalCode = psList.get(0);
					String province = postalCode.getAdminName2();
					String provinceCode = postalCode.getAdminCode2();

					CITY = postalCode.getPlaceName();

					result = new LocationData(CITY, province, provinceCode);

					System.out.println("\tProvince: " + province + "\tProvince code: " + provinceCode);

				} else {
					System.out.println("\tProvince not found!");
				}

			}
			return result;

		} else {
			System.err.println("Empty language!!!! countryCode: " + countryCode);

			if (countryCode.trim().equals("")) {
				System.out.println("ORG: " + org.getLabel() + " has empty countryCode!!!!");
			} else {
				System.err.println("Empty language!!!! countryCode: " + countryCode);
				// System.exit(-1);
			}
		}

		return null;

	}

	public class LocationData {

		String city;
		String urbanUnit;
		String urbanUnitCode;

		public LocationData(String city, String urbanUnit, String urbanUnitCode) {

			this.city = city;
			this.urbanUnit = urbanUnit;
			this.urbanUnitCode = urbanUnitCode;
		}

	}

	private class LanguageData {

		private Map<String, String> dictionaryEN_L;

		private Map<String, String> dictionaryL_L;

		private Map<String, List<PostalCodeItem>> cityPostalCodesL;

		public LanguageData(Map<String, String> dictionaryEN_L,

				Map<String, String> dictionaryL_L,

				Map<String, List<PostalCodeItem>> cityPostalCodesL) {

			this.dictionaryEN_L = dictionaryEN_L;
			this.dictionaryL_L = dictionaryL_L;
			this.cityPostalCodesL = cityPostalCodesL;
		}

		public Map<String, String> getDictionaryEN_L() {
			return dictionaryEN_L;
		}

		public void setDictionaryEN_L(Map<String, String> dictionaryEN_L) {
			this.dictionaryEN_L = dictionaryEN_L;
		}

		public Map<String, String> getDictionaryL_L() {
			return dictionaryL_L;
		}

		public void setDictionaryL_L(Map<String, String> dictionaryL_L) {
			this.dictionaryL_L = dictionaryL_L;
		}

		public Map<String, List<PostalCodeItem>> getCityPostalCodesL() {
			return cityPostalCodesL;
		}

		public void setCityPostalCodesL(Map<String, List<PostalCodeItem>> cityPostalCodesL) {
			this.cityPostalCodesL = cityPostalCodesL;
		}

	}

}
