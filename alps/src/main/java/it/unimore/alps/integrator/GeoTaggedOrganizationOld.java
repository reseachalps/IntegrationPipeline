package it.unimore.alps.integrator;

import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvBindByPosition;

public class GeoTaggedOrganizationOld {
	/*@CsvBindByName
	Integer id;
	@CsvBindByName
	String acronym;
	@CsvBindByName
	String alias;
	@CsvBindByName
	String label;
	@CsvBindByName
	String creationYear;
	@CsvBindByName
	String commercialLabel;
	@CsvBindByName
	String address;
	@CsvBindByName
	String city;
	@CsvBindByName
	String citycode;
	@CsvBindByName
	String country;
	@CsvBindByName
	String countryCode;
	@CsvBindByName
	String postcode;
	@CsvBindByName
	String urbanUnit;
	@CsvBindByName
	String urbanUnitCode;
	@CsvBindByName
	String lat;
	@CsvBindByName
	String lon;
	@CsvBindByName
	String revenueRange;
	@CsvBindByName
	String privateFinanceDate;
	@CsvBindByName
	String employees;
	@CsvBindByName
	String typeCategoryCode;
	@CsvBindByName
	String typeLabel;
	@CsvBindByName
	String typeKind;
	@CsvBindByName
	String isPublic;
	@CsvBindByName
	String leaders;
	@CsvBindByName
	String links;
	@CsvBindByName
	String privateOrgTypeId;
	@CsvBindByName
	String privateOrgTypeLabel;
	@CsvBindByName
	String activities;
	@CsvBindByName
	String relations;
	@CsvBindByName
	String badges;*/
	@CsvBindByName
	Integer id;
	@CsvBindByName
	String address;
	@CsvBindByName
	String alias;
	@CsvBindByName
	String city;
	@CsvBindByName
	String citycode;
	@CsvBindByName
	String commercialLabel;		
	@CsvBindByName
	String country;
	@CsvBindByName
	String countryCode;
	@CsvBindByName
	String creationYear;
	@CsvBindByName
	String privateFinanceDate;
	@CsvBindByName
	String employees;
	@CsvBindByName
	String revenueRange;
	@CsvBindByName
	String isPublic;	
	@CsvBindByName
	String label;
	@CsvBindByName
	String lat;
	@CsvBindByName
	String lon;	
	@CsvBindByName
	String postcode;
	@CsvBindByName
	String typeCategoryCode;
	@CsvBindByName
	String typeKind;
	@CsvBindByName
	String typeLabel;	
	@CsvBindByName
	String urbanUnit;
	@CsvBindByName
	String urbanUnitCode;

	
	public GeoTaggedOrganizationOld() {
	}
	
	/*public GeoTaggedOrganization(Integer id, String acronym, String alias, String label, String creationYear,
			String commercialLabel, String address, String city, String citycode, String country, String countryCode,
			String postcode, String urbanUnit, String urbanUnitCode, String lat, String lon, String revenueRange,
			String privateFinanceDate, String employees, String typeCategoryCode, String typeLabel, String typeKind,
			String isPublic, String leaders, String links, String privateOrgTypeId, String privateOrgTypeLabel,
			String activities, String relations, String badges) {*/
	public GeoTaggedOrganizationOld(Integer id, String alias, String label, String creationYear,
			String commercialLabel, String address, String city, String citycode, String country, String countryCode,
			String postcode, String urbanUnit, String urbanUnitCode, String lat, String lon, String revenueRange,
			String privateFinanceDate, String employees, String typeCategoryCode, String typeLabel, String typeKind,
			String isPublic) {
		super();
		/*this.id = id;
		this.acronym = acronym;
		this.alias = alias;
		this.label = label;
		this.creationYear = creationYear;
		this.commercialLabel = commercialLabel;
		this.address = address;
		this.city = city;
		this.citycode = citycode;
		this.country = country;
		this.countryCode = countryCode;
		this.postcode = postcode;
		this.urbanUnit = urbanUnit;
		this.urbanUnitCode = urbanUnitCode;
		this.lat = lat;
		this.lon = lon;
		this.revenueRange = revenueRange;
		this.privateFinanceDate = privateFinanceDate;
		this.employees = employees;
		this.typeCategoryCode = typeCategoryCode;
		this.typeLabel = typeLabel;
		this.typeKind = typeKind;
		this.isPublic = isPublic;
		this.leaders = leaders;
		this.links = links;
		this.privateOrgTypeId = privateOrgTypeId;
		this.privateOrgTypeLabel = privateOrgTypeLabel;
		this.activities = activities;
		this.relations = relations;
		this.badges = badges;*/
		this.id = id;
		this.address = address;
		this.alias = alias;
		this.city = city;
		this.citycode = citycode;
		this.commercialLabel = commercialLabel;
		this.country = country;
		this.countryCode = countryCode;
		this.creationYear = creationYear;
		this.privateFinanceDate = privateFinanceDate;
		this.employees = employees;
		this.revenueRange = revenueRange;
		this.isPublic = isPublic;
		this.label = label;
		this.lat = lat;
		this.lon = lon;
		this.postcode = postcode;
		this.typeCategoryCode = typeCategoryCode;
		this.typeKind = typeKind;
		this.typeLabel = typeLabel;
		this.urbanUnit = urbanUnit;
		this.urbanUnitCode = urbanUnitCode;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	/*public String getAcronym() {
		return acronym;
	}

	public void setAcronym(String acronym) {
		this.acronym = acronym;
	}*/

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public String getCreationYear() {
		return creationYear;
	}

	public void setCreationYear(String creationYear) {
		this.creationYear = creationYear;
	}

	public String getCommercialLabel() {
		return commercialLabel;
	}

	public void setCommercialLabel(String commercialLabel) {
		this.commercialLabel = commercialLabel;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getCitycode() {
		return citycode;
	}

	public void setCitycode(String citycode) {
		this.citycode = citycode;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getCountryCode() {
		return countryCode;
	}

	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}

	public String getPostcode() {
		return postcode;
	}

	public void setPostcode(String postcode) {
		this.postcode = postcode;
	}

	public String getUrbanUnit() {
		return urbanUnit;
	}

	public void setUrbanUnit(String urbanUnit) {
		this.urbanUnit = urbanUnit;
	}

	public String getUrbanUnitCode() {
		return urbanUnitCode;
	}

	public void setUrbanUnitCode(String urbanUnitCode) {
		this.urbanUnitCode = urbanUnitCode;
	}

	public String getLat() {
		return lat;
	}

	public void setLat(String lat) {
		this.lat = lat;
	}

	public String getLon() {
		return lon;
	}

	public void setLon(String lon) {
		this.lon = lon;
	}

	public String getRevenueRange() {
		return revenueRange;
	}

	public void setRevenueRange(String revenueRange) {
		this.revenueRange = revenueRange;
	}

	public String getPrivateFinanceDate() {
		return privateFinanceDate;
	}

	public void setPrivateFinanceDate(String privateFinanceDate) {
		this.privateFinanceDate = privateFinanceDate;
	}

	public String getEmployees() {
		return employees;
	}

	public void setEmployees(String employees) {
		this.employees = employees;
	}

	public String getTypeCategoryCode() {
		return typeCategoryCode;
	}

	public void setTypeCategoryCode(String typeCategoryCode) {
		this.typeCategoryCode = typeCategoryCode;
	}

	public String getTypeLabel() {
		return typeLabel;
	}

	public void setTypeLabel(String typeLabel) {
		this.typeLabel = typeLabel;
	}

	public String getTypeKind() {
		return typeKind;
	}

	public void setTypeKind(String typeKind) {
		this.typeKind = typeKind;
	}

	public String getIsPublic() {
		return isPublic;
	}

	public void setIsPublic(String isPublic) {
		this.isPublic = isPublic;
	}

	/*public String getLeaders() {
		return leaders;
	}

	public void setLeaders(String leaders) {
		this.leaders = leaders;
	}

	public String getLinks() {
		return links;
	}

	public void setLinks(String links) {
		this.links = links;
	}

	public String getPrivateOrgTypeId() {
		return privateOrgTypeId;
	}

	public void setPrivateOrgTypeId(String privateOrgTypeId) {
		this.privateOrgTypeId = privateOrgTypeId;
	}

	public String getPrivateOrgTypeLabel() {
		return privateOrgTypeLabel;
	}

	public void setPrivateOrgTypeLabel(String privateOrgTypeLabel) {
		this.privateOrgTypeLabel = privateOrgTypeLabel;
	}

	public String getActivities() {
		return activities;
	}

	public void setActivities(String activities) {
		this.activities = activities;
	}

	public String getRelations() {
		return relations;
	}

	public void setRelations(String relations) {
		this.relations = relations;
	}

	public String getBadges() {
		return badges;
	}

	public void setBadges(String badges) {
		this.badges = badges;
	}

//	public String getChildren() {
//		return children;
//	}
//
//	public void setChildren(String children) {
//		this.children = children;
//	}*/
	
	
	

	
	
	
}
