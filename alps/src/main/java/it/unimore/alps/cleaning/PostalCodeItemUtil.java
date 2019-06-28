package it.unimore.alps.cleaning;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PostalCodeItemUtil {

	public Map<String, List<PostalCodeItem>> retrieveMap(List<PostalCodeItem> postalCodes) {

		Map<String, List<PostalCodeItem>> placePostalCodes = new HashMap<>();

		for (PostalCodeItem p : postalCodes) {
			String placeName = p.getPlaceName().toLowerCase();
			if (placePostalCodes.containsKey(placeName)) {
				placePostalCodes.get(placeName).add(p);
			} else {
				List<PostalCodeItem> postalCodesList = new ArrayList<>();
				postalCodesList.add(p);
				placePostalCodes.put(placeName, postalCodesList);
			}
		}

		return placePostalCodes;
	}

	public List<PostalCodeItem> extractPostalCodes(String file) {
		List<PostalCodeItem> postalCodes = new ArrayList<>();

		try {
			File fileDir = new File(file);

			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(fileDir), "UTF8"));

			String str;

			while ((str = in.readLine()) != null) {
				// System.out.println(str);

				if (!str.trim().equals("")) {

					PostalCodeItem ex = transformLineIntoPostalCode(str);
					postalCodes.add(ex);
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

		return postalCodes;
	}

	private PostalCodeItem transformLineIntoPostalCode(String str) {

		String[] elements = str.split("\t", -1);

		String countryCode = parseString(elements[0]); // : iso country code, 2 characters
		String postalCode = parseString(elements[1]); // : varchar(20)
		String placeName = parseString(elements[2]); // : varchar(180)
		String adminName1 = parseString(elements[3]); // : 1. order subdivision (state) varchar(100)
		String adminCode1 = parseString(elements[4]); // : 1. order subdivision (state) varchar(20)
		String adminName2 = parseString(elements[5]); // : 2. order subdivision (county/province) varchar(100)
		String adminCode2 = parseString(elements[6]); // : 2. order subdivision (county/province) varchar(20)
		String adminName3 = parseString(elements[7]); // : 3. order subdivision (community) varchar(100)
		String adminCode3 = parseString(elements[8]); // : 3. order subdivision (community) varchar(20)
		Float latitude = parseFloat(elements[9]);
		; // : estimated latitude (wgs84)
		Float longitude = parseFloat(elements[10]);// : estimated longitude (wgs84)
		Integer accuracy = parseInt(elements[11]);// : accuracy of lat/lng from 1=estimated to 6=centroid

		return new PostalCodeItem(countryCode, postalCode, placeName, adminName1, adminCode1, adminName2, adminCode2,
				adminName3, adminCode3, latitude, longitude, accuracy);
	}

	private Integer parseInt(String input) {
		if (input.trim().equals("")) {
			return null;
		}
		return Integer.parseInt(input);
	}

	private String parseString(String input) {
		if (input.trim().equals("")) {
			return null;
		}
		return input.trim();

	}

	private Float parseFloat(String input) {
		if (input.trim().equals("")) {
			return null;
		}
		return Float.parseFloat(input);

	}

	public static void main(String[] args) {
		System.out.println("START ");
		PostalCodeItemUtil util = new PostalCodeItemUtil();

		List<PostalCodeItem> postalCodes = util.extractPostalCodes("/Users/paolosottovia/Downloads/IT/IT.txt");
		System.out.println("Number of postal codes: " + postalCodes.size());
	}

}
