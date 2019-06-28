package it.unimore.alps.sources.cordis;

import com.opencsv.bean.CsvBindByName;

public class CordisOrganization {
	@CsvBindByName
	String projectRcn;
	@CsvBindByName
	String projectID;
	@CsvBindByName
	String projectAcronym;
	@CsvBindByName
	String role;
	@CsvBindByName
	String id;
	@CsvBindByName
	String name;
	@CsvBindByName
	String shortName;
	@CsvBindByName
	String activityType;
	@CsvBindByName
	String endOfParticipation;
	@CsvBindByName
	String ecContribution;
	@CsvBindByName
	String country;
	@CsvBindByName
	String street;
	@CsvBindByName
	String city;
	@CsvBindByName
	String postCode;
	@CsvBindByName
	String organizationUrl;
	@CsvBindByName
	String contactType;
	@CsvBindByName
	String contactTitle;
	@CsvBindByName
	String contactFirstNames;
	@CsvBindByName
	String contactLastNames;
	@CsvBindByName
	String contactFunction;
	@CsvBindByName
	String contactTelephoneNumber;
	@CsvBindByName
	String contactFaxNumber;
	@CsvBindByName
	String contactEmail;
	
	public CordisOrganization() {
	}

	public CordisOrganization(String projectRcn, String projectID, String projectAcronym, String role, String id,
			String name, String shortName, String activityType, String endOfParticipation, String ecContribution,
			String country, String street, String city, String postCode, String organizationUrl, String contactType,
			String contactTitle, String contactFirstNames, String contactLastNames, String contactFunction,
			String contactTelephoneNumber, String contactFaxNumber, String contactEmail) {
		super();
		this.projectRcn = projectRcn;
		this.projectID = projectID;
		this.projectAcronym = projectAcronym;
		this.role = role;
		this.id = id;
		this.name = name;
		this.shortName = shortName;
		this.activityType = activityType;
		this.endOfParticipation = endOfParticipation;
		this.ecContribution = ecContribution;
		this.country = country;
		this.street = street;
		this.city = city;
		this.postCode = postCode;
		this.organizationUrl = organizationUrl;
		this.contactType = contactType;
		this.contactTitle = contactTitle;
		this.contactFirstNames = contactFirstNames;
		this.contactLastNames = contactLastNames;
		this.contactFunction = contactFunction;
		this.contactTelephoneNumber = contactTelephoneNumber;
		this.contactFaxNumber = contactFaxNumber;
		this.contactEmail = contactEmail;
	}

	public String getProjectRcn() {
		return projectRcn;
	}

	public void setProjectRcn(String projectRcn) {
		this.projectRcn = projectRcn;
	}

	public String getProjectID() {
		return projectID;
	}

	public void setProjectID(String projectID) {
		this.projectID = projectID;
	}

	public String getProjectAcronym() {
		return projectAcronym;
	}

	public void setProjectAcronym(String projectAcronym) {
		this.projectAcronym = projectAcronym;
	}

	public String getRole() {
		return role;
	}

	public void setRole(String role) {
		this.role = role;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getShortName() {
		return shortName;
	}

	public void setShortName(String shortName) {
		this.shortName = shortName;
	}

	public String getActivityType() {
		return activityType;
	}

	public void setActivityType(String activityType) {
		this.activityType = activityType;
	}

	public String getEndOfParticipation() {
		return endOfParticipation;
	}

	public void setEndOfParticipation(String endOfParticipation) {
		this.endOfParticipation = endOfParticipation;
	}

	public String getEcContribution() {
		return ecContribution;
	}

	public void setEcContribution(String ecContribution) {
		this.ecContribution = ecContribution;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getStreet() {
		return street;
	}

	public void setStreet(String street) {
		this.street = street;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getPostCode() {
		return postCode;
	}

	public void setPostCode(String postCode) {
		this.postCode = postCode;
	}

	public String getOrganizationUrl() {
		return organizationUrl;
	}

	public void setOrganizationUrl(String organizationUrl) {
		this.organizationUrl = organizationUrl;
	}

	public String getContactType() {
		return contactType;
	}

	public void setContactType(String contactType) {
		this.contactType = contactType;
	}

	public String getContactTitle() {
		return contactTitle;
	}

	public void setContactTitle(String contactTitle) {
		this.contactTitle = contactTitle;
	}

	public String getContactFirstNames() {
		return contactFirstNames;
	}

	public void setContactFirstNames(String contactFirstNames) {
		this.contactFirstNames = contactFirstNames;
	}

	public String getContactLastNames() {
		return contactLastNames;
	}

	public void setContactLastNames(String contactLastNames) {
		this.contactLastNames = contactLastNames;
	}

	public String getContactFunction() {
		return contactFunction;
	}

	public void setContactFunction(String contactFunction) {
		this.contactFunction = contactFunction;
	}

	public String getContactTelephoneNumber() {
		return contactTelephoneNumber;
	}

	public void setContactTelephoneNumber(String contactTelephoneNumber) {
		this.contactTelephoneNumber = contactTelephoneNumber;
	}

	public String getContactFaxNumber() {
		return contactFaxNumber;
	}

	public void setContactFaxNumber(String contactFaxNumber) {
		this.contactFaxNumber = contactFaxNumber;
	}

	public String getContactEmail() {
		return contactEmail;
	}

	public void setContactEmail(String contactEmail) {
		this.contactEmail = contactEmail;
	}

}
