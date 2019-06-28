package it.unimore.alps.sql.model;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import javax.persistence.*;

import org.eclipse.persistence.annotations.CascadeOnDelete;
import org.eclipse.persistence.jpa.config.Cascade;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The persistent class for the organization database table.
 * 
 */
@CascadeOnDelete
@Entity
@NamedQuery(name = "Organization.findAll", query = "SELECT o FROM Organization o")
public class Organization implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private int id;

	// private String acronym;
	@ElementCollection
	@CollectionTable(name = "acronyms")
	private List<String> acronyms;

	private String address;

	private String alias;

	private String city;

	@Column(name = "city_code")
	private String cityCode;

	@Column(name = "commercial_label")
	private String commercialLabel;

	private String country;

	@Column(name = "country_code")
	private String countryCode;

	@Temporal(TemporalType.DATE)
	@Column(name = "creation_year")
	private Date creationYear;

	@Temporal(TemporalType.DATE)
	@Column(name = "finance_private_date")
	private Date financePrivateDate;

	@Column(name = "finance_private_employees")
	private String financePrivateEmployees;

	@Column(name = "finance_private_revenue_range")
	private String financePrivateRevenueRange;

	@Column(name = "is_public")
	private String isPublic = "undefined";

	private String label;

	private Float lat=null;

	private Float lon=null;	

	private String postcode;

	@Column(name = "type_category_code")
	private String typeCategoryCode;

	@Column(name = "type_kind")
	private String typeKind;

	@Column(name = "type_label")
	private String typeLabel;

	@Column(name = "urban_unit")
	private String urbanUnit;

	@Column(name = "urban_unit_code")
	private String urbanUnitCode;
	
	
	
	private String nutsLevel1;
	
	private String nutsLevel2;
	
	private String nutsLevel3;
	
	

	// uni-directional many-to-many association to Activity
	@ManyToMany(cascade = { CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.ALL })
	@JoinTable(name = "organization_activities", joinColumns = {
			@JoinColumn(name = "organization_id") }, inverseJoinColumns = { @JoinColumn(name = "activity_code") })
	@CascadeOnDelete
	private List<OrganizationActivity> activities;

	// uni-directional many-to-many association to Badge
	@ManyToMany(cascade = { CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.ALL })
	@JoinTable(name = "organization_badges", joinColumns = {
			@JoinColumn(name = "organization_id") }, inverseJoinColumns = { @JoinColumn(name = "badge_id") })
	@CascadeOnDelete
	private List<Badge> badges;

	// uni-directional many-to-many association to Leader
	@ManyToMany(cascade = { CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.ALL })
	@JoinTable(name = "organization_leaders", joinColumns = {
			@JoinColumn(name = "organization_id") }, inverseJoinColumns = { @JoinColumn(name = "leader_id") })
	@CascadeOnDelete
	private List<Leader> leaders;

	// uni-directional many-to-many association to Link
	@ManyToMany(cascade = { CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.ALL }, fetch=FetchType.EAGER)
	@JoinTable(name = "organization_links", joinColumns = {
			@JoinColumn(name = "organization_id") }, inverseJoinColumns = { @JoinColumn(name = "link_id") })
	@CascadeOnDelete
	private List<Link> links;
	
	
	// uni-directional many-to-many association to alternativeNames
		@ManyToMany(cascade = { CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.ALL })
		@JoinTable(name = "alternativenames_links", joinColumns = {
				@JoinColumn(name = "organization_id") }, inverseJoinColumns = { @JoinColumn(name = "name_id") })
		@CascadeOnDelete
		private List<AlternativeName> alternativeNames;

	// uni-directional many-to-many association to Organization
	@ManyToMany(cascade = { CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.ALL })
	@JoinTable(name = "organization_hierarchies", joinColumns = { @JoinColumn(name = "father") }, inverseJoinColumns = {
			@JoinColumn(name = "child") })
	@CascadeOnDelete
	private List<Organization> childrenOrganizations;

	// uni-directional many-to-one association to OrganizationType
	@ManyToOne(cascade = { CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.ALL })
	@JoinColumn(name = "private_type")
	private OrganizationType organizationType;

	// uni-directional many-to-many association to Project
	@ManyToMany(cascade = { CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.ALL })
	@JoinTable(name = "project_organizations", joinColumns = {
			@JoinColumn(name = "organization_id") }, inverseJoinColumns = { @JoinColumn(name = "project_id") })
	@CascadeOnDelete
	private List<Project> projects;

	// bi-directional many-to-one association to OrganizationExtraField
	@OneToMany(mappedBy = "organization", cascade = { CascadeType.PERSIST, CascadeType.REMOVE,
			CascadeType.ALL }, orphanRemoval = true)
	private List<OrganizationExtraField> organizationExtraFields;

	// bi-directional many-to-one association to OrganizationIdentifier
	@OneToMany(mappedBy = "organization", cascade = { CascadeType.PERSIST, CascadeType.REMOVE,
			CascadeType.ALL }, orphanRemoval = true)
	private List<OrganizationIdentifier> organizationIdentifiers;

	// bi-directional many-to-one association to OrganizationRelation
	@OneToMany(mappedBy = "organization", cascade = { CascadeType.PERSIST, CascadeType.REMOVE,
			CascadeType.ALL }, orphanRemoval = true)
	private List<OrganizationRelation> organizationRelations;

	// uni-directional many-to-many association to Source
	@ManyToMany(cascade = { CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.ALL }, fetch=FetchType.EAGER)
	@JoinTable(name = "organization_sources", joinColumns = {
			@JoinColumn(name = "organization_id") }, inverseJoinColumns = { @JoinColumn(name = "source_id") })
	private List<Source> sources;

	// uni-directional many-to-many association to Source
	@ManyToMany(cascade = { CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.ALL })
	@JoinTable(name = "org_address_sources", joinColumns = {
			@JoinColumn(name = "organization_id") }, inverseJoinColumns = { @JoinColumn(name = "source_id") })
	private List<Source> addressSources;

	// bi-directional many-to-one association to SpinoffFrom
	@OneToMany(mappedBy = "organization", cascade = { CascadeType.PERSIST, CascadeType.REMOVE,
			CascadeType.ALL }, orphanRemoval = true)
	@CascadeOnDelete
	private List<SpinoffFrom> spinoffFroms;

	// uni-directional many-to-many association to Publication
	@ManyToMany(cascade = { CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.ALL })
	@JoinTable(name = "publication_organization", joinColumns = {
			@JoinColumn(name = "organization_id") }, inverseJoinColumns = { @JoinColumn(name = "publication_id") })
	private List<Publication> publications;

	// uni-directional many-to-many association to Person
	@ManyToMany(cascade = { CascadeType.PERSIST, CascadeType.REMOVE, CascadeType.ALL })
	@JoinTable(name = "person_organization", joinColumns = {
			@JoinColumn(name = "organization_id") }, inverseJoinColumns = { @JoinColumn(name = "person_id") })
	private List<Person> people;

	public Organization() {
	}

	public int getId() {
		return this.id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public List<String> getAcronyms() {
		return this.acronyms;
	}

	public void setAcronyms(List<String> acronyms) {
		this.acronyms = acronyms;
	}

	public String getAddress() {
		return this.address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getAlias() {
		return this.alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public String getCity() {
		return this.city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getCityCode() {
		return this.cityCode;
	}

	public void setCityCode(String cityCode) {
		this.cityCode = cityCode;
	}

	public String getCommercialLabel() {
		return this.commercialLabel;
	}

	public void setCommercialLabel(String commercialLabel) {
		this.commercialLabel = commercialLabel;
	}

	public String getCountry() {
		return this.country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getCountryCode() {
		return this.countryCode;
	}

	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}

	public Date getCreationYear() {
		return this.creationYear;
	}

	public void setCreationYear(Date creationYear) {
		this.creationYear = creationYear;
	}

	public Date getFinancePrivateDate() {
		return this.financePrivateDate;
	}

	public void setFinancePrivateDate(Date financePrivateDate) {
		this.financePrivateDate = financePrivateDate;
	}

	public String getFinancePrivateEmployees() {
		return this.financePrivateEmployees;
	}

	public void setFinancePrivateEmployees(String financePrivateEmployees) {
		this.financePrivateEmployees = financePrivateEmployees;
	}

	public String getFinancePrivateRevenueRange() {
		return this.financePrivateRevenueRange;
	}

	public void setFinancePrivateRevenueRange(String financePrivateRevenueRange) {
		this.financePrivateRevenueRange = financePrivateRevenueRange;
	}

	public String getIsPublic() {
		return this.isPublic;
	}

	public void setIsPublic(String isPublic) {
		this.isPublic = isPublic;
	}

	public String getLabel() {
		return this.label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public Float getLat() {
		return this.lat;
	}

	public void setLat(float lat) {
		this.lat = lat;
	}

	public Float getLon() {
		return this.lon;
	}

	public void setLon(float lon) {
		this.lon = lon;
	}

	public String getPostcode() {
		return this.postcode;
	}

	public void setPostcode(String postcode) {
		this.postcode = postcode;
	}

	public String getTypeCategoryCode() {
		return this.typeCategoryCode;
	}

	public void setTypeCategoryCode(String typeCategoryCode) {
		this.typeCategoryCode = typeCategoryCode;
	}

	public String getTypeKind() {
		return this.typeKind;
	}

	public void setTypeKind(String typeKind) {
		this.typeKind = typeKind;
	}

	public String getTypeLabel() {
		return this.typeLabel;
	}

	public void setTypeLabel(String typeLabel) {
		this.typeLabel = typeLabel;
	}

	public String getUrbanUnit() {
		return this.urbanUnit;
	}

	public void setUrbanUnit(String urbanUnit) {
		this.urbanUnit = urbanUnit;
	}

	public String getUrbanUnitCode() {
		return this.urbanUnitCode;
	}

	public void setUrbanUnitCode(String urbanUnitCode) {
		this.urbanUnitCode = urbanUnitCode;
	}

	public List<OrganizationActivity> getActivities() {
		return this.activities;
	}

	public void setActivities(List<OrganizationActivity> activities) {
		this.activities = activities;
	}

	public List<Badge> getBadges() {
		return this.badges;
	}

	public void setBadges(List<Badge> badges) {
		this.badges = badges;
	}

	public List<Leader> getLeaders() {
		return this.leaders;
	}

	public void setLeaders(List<Leader> leaders) {
		this.leaders = leaders;
	}

	public List<Publication> getPublications() {
		return this.publications;
	}

	public void setPublications(List<Publication> publications) {
		this.publications = publications;
	}

	public List<Person> getPeople() {
		return this.people;
	}

	public void setPeople(List<Person> people) {
		this.people = people;
	}

	public List<Link> getLinks() {
		return this.links;
	}

	public void setLinks(List<Link> links) {
		this.links = links;
	}

	public List<AlternativeName> getAlternativeNames() {
		return alternativeNames;
	}

	public void setAlternativeNames(List<AlternativeName> alternativeNames) {
		this.alternativeNames = alternativeNames;
	}

	public List<Organization> getChildrenOrganizations() {
		return this.childrenOrganizations;
	}

	public void setChildrenOrganizations(List<Organization> childrenOrganizations) {
		this.childrenOrganizations = childrenOrganizations;
	}

	public OrganizationType getOrganizationType() {
		return this.organizationType;
	}

	public void setOrganizationType(OrganizationType organizationType) {
		this.organizationType = organizationType;
	}

	public List<Project> getProjects() {
		return this.projects;
	}

	public void setProjects(List<Project> projects) {
		this.projects = projects;
	}

	public List<OrganizationExtraField> getOrganizationExtraFields() {
		return this.organizationExtraFields;
	}

	public void setOrganizationExtraFields(List<OrganizationExtraField> organizationExtraFields) {
		this.organizationExtraFields = organizationExtraFields;
	}

	public OrganizationExtraField addOrganizationExtraField(OrganizationExtraField organizationExtraField) {
		getOrganizationExtraFields().add(organizationExtraField);
		organizationExtraField.setOrganization(this);

		return organizationExtraField;
	}

	public OrganizationExtraField removeOrganizationExtraField(OrganizationExtraField organizationExtraField) {
		getOrganizationExtraFields().remove(organizationExtraField);
		organizationExtraField.setOrganization(null);

		return organizationExtraField;
	}

	public List<OrganizationIdentifier> getOrganizationIdentifiers() {
		return this.organizationIdentifiers;
	}

	public void setOrganizationIdentifiers(List<OrganizationIdentifier> organizationIdentifiers) {
		this.organizationIdentifiers = organizationIdentifiers;
	}

	public OrganizationIdentifier addOrganizationIdentifier(OrganizationIdentifier organizationIdentifier) {
		getOrganizationIdentifiers().add(organizationIdentifier);
		organizationIdentifier.setOrganization(this);

		return organizationIdentifier;
	}

	public OrganizationIdentifier removeOrganizationIdentifier(OrganizationIdentifier organizationIdentifier) {
		getOrganizationIdentifiers().remove(organizationIdentifier);
		organizationIdentifier.setOrganization(null);

		return organizationIdentifier;
	}

	public List<OrganizationRelation> getOrganizationRelations() {
		return this.organizationRelations;
	}

	public void setOrganizationRelations(List<OrganizationRelation> organizationRelations) {
		this.organizationRelations = organizationRelations;
	}

	public OrganizationRelation addOrganizationRelation(OrganizationRelation organizationRelation) {
		getOrganizationRelations().add(organizationRelation);
		organizationRelation.setOrganization(this);

		return organizationRelation;
	}

	public OrganizationRelation removeOrganizationRelation(OrganizationRelation organizationRelation) {
		getOrganizationRelations().remove(organizationRelation);
		organizationRelation.setOrganization(null);

		return organizationRelation;
	}

	public List<Source> getSources() {
		return this.sources;
	}

	public void setSources(List<Source> sources) {
		this.sources = sources;
	}

	public List<Source> getAddressSources() {
		return this.addressSources;
	}

	public void setAddressSources(List<Source> sources) {
		this.addressSources = sources;
	}

	public List<SpinoffFrom> getSpinoffFroms() {
		return this.spinoffFroms;
	}

	public void setSpinoffFroms(List<SpinoffFrom> spinoffFroms) {
		this.spinoffFroms = spinoffFroms;
	}

	public SpinoffFrom addSpinoffFrom(SpinoffFrom spinoffFrom) {
		getSpinoffFroms().add(spinoffFrom);
		spinoffFrom.setOrganization(this);

		return spinoffFrom;
	}

	public SpinoffFrom removeSpinoffFrom(SpinoffFrom spinoffFrom) {
		getSpinoffFroms().remove(spinoffFrom);
		spinoffFrom.setOrganization(null);

		return spinoffFrom;
	}
	

	public String getNutsLevel1() {
		return nutsLevel1;
	}

	public void setNutsLevel1(String nutsLevel1) {
		this.nutsLevel1 = nutsLevel1;
	}

	public String getNutsLevel2() {
		return nutsLevel2;
	}

	public void setNutsLevel2(String nutsLevel2) {
		this.nutsLevel2 = nutsLevel2;
	}

	public String getNutsLevel3() {
		return nutsLevel3;
	}

	public void setNutsLevel3(String nutsLevel3) {
		this.nutsLevel3 = nutsLevel3;
	}

	public Map<String, String> retrieveValues() {
		Map<String, String> values = new HashMap<>();

		values.put("label", getValue(this.label));
		values.put("acronyms", getValue(this.acronyms.toString()));
		values.put("address", getValue(this.address));
		values.put("alias", getValue(this.alias));
		values.put("city", getValue(this.city));
		values.put("cityCode", getValue(this.cityCode));
		values.put("commercialLabel", getValue(this.commercialLabel));
		values.put("country", getValue(this.country));
		values.put("countryCode", getValue(this.countryCode));
		values.put("postcode", getValue(this.postcode));
		values.put("links", getValue(this.links.toString()));
		// SimpleDateFormat formatterDate = new SimpleDateFormat("yyyy-MM-dd");
		// SimpleDateFormat formatterYear = new SimpleDateFormat("yyyy");

		// values.put("creationYear",
		// getValue(formatterYear.format(this.creationYear)));
		// values.put("financePrivateDate",
		// getValue(formatterDate.format(this.financePrivateDate)));
		values.put("financePrivateEmployees", getValue(this.financePrivateEmployees));
		values.put("financePrivateRevenueRange", getValue(this.financePrivateRevenueRange));
		values.put("financePrivateRevenueRange", getValue(this.financePrivateRevenueRange));
		/*
		 * String isPublic = "false"; if (this.isPublic == 1) { isPublic = "true"; }
		 */
		values.put("isPublic", getValue(this.isPublic));
		values.put("lat", getValue("" + this.lat));
		values.put("lon", getValue("" + this.lon));
		values.put("typeCategoryCode", getValue(this.typeCategoryCode));
		values.put("typeKind", getValue(this.typeKind));
		values.put("typeLabel", getValue(this.typeLabel));
		values.put("urbanUnit", getValue(this.urbanUnit));
		values.put("urbanUnitCode", getValue(this.urbanUnitCode));

		return values;

	}

	private String getValue(String value) {

		if (value == null) {
			return null;
		} else if (value.trim().equals("")) {
			return null;
		} else if (value.trim().toLowerCase().equals("null")) {
			return null;
		} else {
			return value.trim();
		}
		// return value;
	}

	@Override
	public String toString() {
		return this.id + "\t" + this.label + "\t" + this.address + "\t" + this.city;
	}

}