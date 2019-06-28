package it.unimore.alps.sources.cordis;

import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvBindByPosition;

public class CordisProject {

	@CsvBindByPosition(position = 0)
	String rcn;
	@CsvBindByPosition(position = 1)
	String id;
	@CsvBindByPosition(position = 2)
	String acronym;
	@CsvBindByPosition(position = 3)
	String status;
	@CsvBindByPosition(position = 4)
	String programme;
	@CsvBindByPosition(position = 5)
	String topics;
	@CsvBindByPosition(position = 6)
	String frameworkProgramme;
	@CsvBindByPosition(position = 7)
	String title;
	@CsvBindByPosition(position = 8)
	String startDate;
	@CsvBindByPosition(position = 9)
	String endDate;
	@CsvBindByPosition(position = 10)
	String projectUrl;
	@CsvBindByPosition(position = 11)
	String objective;
	@CsvBindByPosition(position = 12)
	String totalCost;
	@CsvBindByPosition(position = 13)
	String ecMaxContribution;
	@CsvBindByPosition(position = 14)
	String call;
	@CsvBindByPosition(position = 15)
	String fundingScheme;
	@CsvBindByPosition(position = 16)
	String coordinator;
	@CsvBindByPosition(position = 17)
	String coordinatorCountry;
	@CsvBindByPosition(position = 18)
	String participants;
	@CsvBindByPosition(position = 19)
	String participantCountries;
	@CsvBindByPosition(position = 20)
	String subjects;

	public CordisProject() {
	}

	public CordisProject(String rcn, String id, String acronym, String status, String programme, String topics,
			String frameworkProgramme, String title, String startDate, String endDate, String projectUrl,
			String objective, String totalCost, String ecMaxContribution, String call, String fundingScheme,
			String coordinator, String coordinatorCountry, String participants, String participantCountries,
			String subjects) {
		super();
		this.rcn = rcn;
		this.id = id;
		this.acronym = acronym;
		this.status = status;
		this.programme = programme;
		this.topics = topics;
		this.frameworkProgramme = frameworkProgramme;
		this.title = title;
		this.startDate = startDate;
		this.endDate = endDate;
		this.projectUrl = projectUrl;
		this.objective = objective;
		this.totalCost = totalCost;
		this.ecMaxContribution = ecMaxContribution;
		this.call = call;
		this.fundingScheme = fundingScheme;
		this.coordinator = coordinator;
		this.coordinatorCountry = coordinatorCountry;
		this.participants = participants;
		this.participantCountries = participantCountries;
		this.subjects = subjects;
	}

	public String getRcn() {
		return rcn;
	}

	public void setRcn(String rcn) {
		this.rcn = rcn;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getAcronym() {
		return acronym;
	}

	public void setAcronym(String acronym) {
		this.acronym = acronym;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getProgramme() {
		return programme;
	}

	public void setProgramme(String programme) {
		this.programme = programme;
	}

	public String getTopics() {
		return topics;
	}

	public void setTopics(String topics) {
		this.topics = topics;
	}

	public String getFrameworkProgramme() {
		return frameworkProgramme;
	}

	public void setFrameworkProgramme(String frameworkProgramme) {
		this.frameworkProgramme = frameworkProgramme;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getStartDate() {
		return startDate;
	}

	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}

	public String getProjectUrl() {
		return projectUrl;
	}

	public void setProjectUrl(String projectUrl) {
		this.projectUrl = projectUrl;
	}

	public String getObjective() {
		return objective;
	}

	public void setObjective(String objective) {
		this.objective = objective;
	}

	public String getTotalCost() {
		return totalCost;
	}

	public void setTotalCost(String totalCost) {
		this.totalCost = totalCost;
	}

	public String getEcMaxContribution() {
		return ecMaxContribution;
	}

	public void setEcMaxContribution(String ecMaxContribution) {
		this.ecMaxContribution = ecMaxContribution;
	}

	public String getCall() {
		return call;
	}

	public void setCall(String call) {
		this.call = call;
	}

	public String getFundingScheme() {
		return fundingScheme;
	}

	public void setFundingScheme(String fundingScheme) {
		this.fundingScheme = fundingScheme;
	}

	public String getCoordinator() {
		return coordinator;
	}

	public void setCoordinator(String coordinator) {
		this.coordinator = coordinator;
	}

	public String getCoordinatorCountry() {
		return coordinatorCountry;
	}

	public void setCoordinatorCountry(String coordinatorCountry) {
		this.coordinatorCountry = coordinatorCountry;
	}

	public String getParticipants() {
		return participants;
	}

	public void setParticipants(String participants) {
		this.participants = participants;
	}

	public String getParticipantCountries() {
		return participantCountries;
	}

	public void setParticipantCountries(String participantCountries) {
		this.participantCountries = participantCountries;
	}

	public String getSubjects() {
		return subjects;
	}

	public void setSubjects(String subjects) {
		this.subjects = subjects;
	}

}
