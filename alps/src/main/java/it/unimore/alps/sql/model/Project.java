package it.unimore.alps.sql.model;

import java.io.Serializable;
import javax.persistence.*;

import org.eclipse.persistence.annotations.CascadeOnDelete;

import java.util.Date;
import java.util.List;


/**
 * The persistent class for the project database table.
 * 
 */
@CascadeOnDelete
@Entity
@NamedQuery(name="Project.findAll", query="SELECT p FROM Project p")
public class Project implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy=GenerationType.AUTO)
	private int id;

	private String acronym;

	private String budget;

	@Column(name="call_id")
	private String callId;

	@Column(name="call_label")
	private String callLabel;

	@Column(columnDefinition = "TEXT")
	private String description;

	private String duration;

	@Column(columnDefinition = "TEXT")
	private String label;

	private String month;

	@Temporal(TemporalType.DATE)
	@Column(name="start_date")
	private Date startDate;

	private String type;

	private String url;

	private String year;

	//bi-directional many-to-one association to ProjectExtraField
	@OneToMany(mappedBy="project",cascade={CascadeType.PERSIST,CascadeType.REMOVE},orphanRemoval = true)
	@CascadeOnDelete
	private List<ProjectExtraField> projectExtraFields;

	//bi-directional many-to-one association to ProjectIdentifier
	@OneToMany(mappedBy="project",cascade={CascadeType.PERSIST,CascadeType.REMOVE},orphanRemoval = true)
	@CascadeOnDelete
	private List<ProjectIdentifier> projectIdentifiers;

	//uni-directional many-to-many association to Source
	@ManyToMany(cascade={CascadeType.PERSIST,CascadeType.REMOVE})
	@JoinTable(
		name="project_sources"
		, joinColumns={
			@JoinColumn(name="project_id")
			}
		, inverseJoinColumns={
			@JoinColumn(name="source_id")
			}
		)
	@CascadeOnDelete
	private List<Source> sources;

	//uni-directional many-to-many association to Theme
	@ManyToMany(cascade={CascadeType.PERSIST,CascadeType.REMOVE})
	@JoinTable(
		name="project_themes"
		, joinColumns={
			@JoinColumn(name="project_id")
			}
		, inverseJoinColumns={
			@JoinColumn(name="theme_id")
			}
		)
	@CascadeOnDelete
	private List<Theme> themes;
	
	
	@ManyToMany(cascade={CascadeType.PERSIST,CascadeType.REMOVE})
	@JoinTable(
		name="project_external"
		, joinColumns={
			@JoinColumn(name="project_id")
			}
		, inverseJoinColumns={
			@JoinColumn(name="external_id")
			}
		)
	@CascadeOnDelete
	private List<ExternalParticipant> externalParticipants;
	
	//uni-directional many-to-many association to Thematic
	@ManyToMany(cascade={CascadeType.PERSIST,CascadeType.REMOVE})
	@JoinTable(
		name="project_thematics"
		, joinColumns={
			@JoinColumn(name="project_id")
			}
		, inverseJoinColumns={
			@JoinColumn(name="thematic_id")
			}
		)
	@CascadeOnDelete
	private List<Thematic> thematics;

	public Project() {
	}

	public int getId() {
		return this.id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getAcronym() {
		return this.acronym;
	}

	public void setAcronym(String acronym) {
		this.acronym = acronym;
	}

	public String getBudget() {
		return this.budget;
	}

	public void setBudget(String budget) {
		this.budget = budget;
	}

	public String getCallId() {
		return this.callId;
	}

	public void setCallId(String callId) {
		this.callId = callId;
	}

	public String getCallLabel() {
		return this.callLabel;
	}

	public void setCallLabel(String callLabel) {
		this.callLabel = callLabel;
	}

	public String getDescription() {
		return this.description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getDuration() {
		return this.duration;
	}

	public void setDuration(String duration) {
		this.duration = duration;
	}

	public String getLabel() {
		return this.label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public String getMonth() {
		return this.month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public Date getStartDate() {
		return this.startDate;
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

	public String getType() {
		return this.type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getUrl() {
		return this.url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getYear() {
		return this.year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public List<ProjectExtraField> getProjectExtraFields() {
		return this.projectExtraFields;
	}

	public void setProjectExtraFields(List<ProjectExtraField> projectExtraFields) {
		this.projectExtraFields = projectExtraFields;
	}

	public ProjectExtraField addProjectExtraField(ProjectExtraField projectExtraField) {
		getProjectExtraFields().add(projectExtraField);
		projectExtraField.setProject(this);

		return projectExtraField;
	}

	public ProjectExtraField removeProjectExtraField(ProjectExtraField projectExtraField) {
		getProjectExtraFields().remove(projectExtraField);
		projectExtraField.setProject(null);

		return projectExtraField;
	}

	public List<ProjectIdentifier> getProjectIdentifiers() {
		return this.projectIdentifiers;
	}

	public void setProjectIdentifiers(List<ProjectIdentifier> projectIdentifiers) {
		this.projectIdentifiers = projectIdentifiers;
	}

	public ProjectIdentifier addProjectIdentifier(ProjectIdentifier projectIdentifier) {
		getProjectIdentifiers().add(projectIdentifier);
		projectIdentifier.setProject(this);

		return projectIdentifier;
	}

	public ProjectIdentifier removeProjectIdentifier(ProjectIdentifier projectIdentifier) {
		getProjectIdentifiers().remove(projectIdentifier);
		projectIdentifier.setProject(null);

		return projectIdentifier;
	}

	public List<Source> getSources() {
		return this.sources;
	}

	public void setSources(List<Source> sources) {
		this.sources = sources;
	}

	public List<Theme> getThemes() {
		return this.themes;
	}

	public void setThemes(List<Theme> themes) {
		this.themes = themes;
	}
	
	public List<Thematic> getThematics() {
		return this.thematics;
	}

	public void setThematics(List<Thematic> thematics) {
		this.thematics = thematics;
	}

	public List<ExternalParticipant> getExternalParticipants() {
		return externalParticipants;
	}

	public void setExternalParticipants(List<ExternalParticipant> externalParticipants) {
		this.externalParticipants = externalParticipants;
	}
	
	
	

}