package it.unimore.alps.cleaning;

public class PostalCodeItem {

	String countryCode; // : iso country code, 2 characters
	String postalCode; // : varchar(20)
	String placeName; // : varchar(180)
	String adminName1; // : 1. order subdivision (state) varchar(100)
	String adminCode1; // : 1. order subdivision (state) varchar(20)
	String adminName2; // : 2. order subdivision (county/province) varchar(100)
	String adminCode2; // : 2. order subdivision (county/province) varchar(20)
	String adminName3; // : 3. order subdivision (community) varchar(100)
	String adminCode3; // : 3. order subdivision (community) varchar(20)
	Float latitude; // : estimated latitude (wgs84)
	Float longitude;// : estimated longitude (wgs84)
	Integer accuracy;// : accuracy of lat/lng from 1=estimated to 6=centroid

	public PostalCodeItem(String countryCode, String postalCode, String placeName, String adminName1, String adminCode1,
			String adminName2, String adminCode2, String adminName3, String adminCode3, Float latitude, Float longitude,
			Integer accuracy) {
		super();
		this.countryCode = countryCode;
		this.postalCode = postalCode;
		this.placeName = placeName;
		this.adminName1 = adminName1;
		this.adminCode1 = adminCode1;
		this.adminName2 = adminName2;
		this.adminCode2 = adminCode2;
		this.adminName3 = adminName3;
		this.adminCode3 = adminCode3;
		this.latitude = latitude;
		this.longitude = longitude;
		this.accuracy = accuracy;
	}

	public String getCountryCode() {
		return countryCode;
	}

	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}

	public String getPostalCode() {
		return postalCode;
	}

	public void setPostalCode(String postalCode) {
		this.postalCode = postalCode;
	}

	public String getPlaceName() {
		return placeName;
	}

	public void setPlaceName(String placeName) {
		this.placeName = placeName;
	}

	public String getAdminName1() {
		return adminName1;
	}

	public void setAdminName1(String adminName1) {
		this.adminName1 = adminName1;
	}

	public String getAdminCode1() {
		return adminCode1;
	}

	public void setAdminCode1(String adminCode1) {
		this.adminCode1 = adminCode1;
	}

	public String getAdminName2() {
		return adminName2;
	}

	public void setAdminName2(String adminName2) {
		this.adminName2 = adminName2;
	}

	public String getAdminCode2() {
		return adminCode2;
	}

	public void setAdminCode2(String adminCode2) {
		this.adminCode2 = adminCode2;
	}

	public String getAdminName3() {
		return adminName3;
	}

	public void setAdminName3(String adminName3) {
		this.adminName3 = adminName3;
	}

	public String getAdminCode3() {
		return adminCode3;
	}

	public void setAdminCode3(String adminCode3) {
		this.adminCode3 = adminCode3;
	}

	public Float getLatitude() {
		return latitude;
	}

	public void setLatitude(Float latitude) {
		this.latitude = latitude;
	}

	public Float getLongitude() {
		return longitude;
	}

	public void setLongitude(Float longitude) {
		this.longitude = longitude;
	}

	public Integer getAccuracy() {
		return accuracy;
	}

	public void setAccuracy(Integer accuracy) {
		this.accuracy = accuracy;
	}

}
