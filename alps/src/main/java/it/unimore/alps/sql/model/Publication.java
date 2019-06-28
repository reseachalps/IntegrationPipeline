package it.unimore.alps.sql.model;

import java.io.Serializable;

import javax.persistence.*;

import org.eclipse.persistence.annotations.CascadeOnDelete;

import java.util.Date;
import java.util.List;

/**
 * The persistent class for the organization database table.
 * 
 */
@CascadeOnDelete
@Entity
@NamedQuery(name = "Publication.findAll", query = "SELECT o FROM Publication o")
public class Publication implements Serializable {
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Publication other = (Publication) obj;
		if (id != other.id)
			return false;
		return true;
	}

	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE)
	private int id;

	@Column(columnDefinition = "TEXT")
	private String title;

	@Column(columnDefinition = "TEXT")
	private String subtitle;

	private String location_type;

	@Column(columnDefinition = "TEXT")
	private String location_name;

	@Temporal(TemporalType.DATE)
	private Date publicationDate;

	@Column(columnDefinition = "TEXT")
	private String description;

	@Column(columnDefinition = "TEXT")
	private String url;
	
	private String type;

	// uni-directional many-to-many association to Source
	@ManyToMany(cascade = { CascadeType.PERSIST, CascadeType.REMOVE })
	@JoinTable(name = "publication_sources", joinColumns = {
			@JoinColumn(name = "publication_id") }, inverseJoinColumns = { @JoinColumn(name = "source_id") })
	private List<Source> sources;

	// uni-directional many-to-many association to Project
	@ManyToMany(cascade = { CascadeType.PERSIST, CascadeType.REMOVE })
	@JoinTable(name = "project_publications", joinColumns = {
			@JoinColumn(name = "publication_id") }, inverseJoinColumns = { @JoinColumn(name = "project_id") })
	@CascadeOnDelete
	private List<Project> projects;

	// bi-directional many-to-one association to PublicationIdentifier
	@OneToMany(mappedBy = "publication", cascade = { CascadeType.PERSIST, CascadeType.REMOVE }, orphanRemoval = true)
	private List<PublicationIdentifier> publicationIdentifiers;

	// uni-directional many-to-many association to Person
	@ManyToMany(cascade = { CascadeType.PERSIST, CascadeType.REMOVE })
	@JoinTable(name = "publication_authors", joinColumns = {
			@JoinColumn(name = "publication_id") }, inverseJoinColumns = { @JoinColumn(name = "person_id") })
	@CascadeOnDelete
	private List<Person> authors;

	// uni-directional many-to-many association to Thematic
	@ManyToMany(cascade = { CascadeType.PERSIST, CascadeType.REMOVE })
	@JoinTable(name = "publication_thematics", joinColumns = {
			@JoinColumn(name = "publication_id") }, inverseJoinColumns = { @JoinColumn(name = "thematic_id") })
	@CascadeOnDelete
	private List<Thematic> thematics;

	public Publication() {
	}

	public int getId() {
		return this.id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getTitle() {
		return this.title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getSubtitle() {
		return this.subtitle;
	}

	public void setSubtitle(String subtitle) {
		this.subtitle = subtitle;
	}

	public String getLocationType() {
		return this.location_type;
	}

	public void setLocationType(String location_type) {
		this.location_type = location_type;
	}

	public String getLocationName() {
		return this.location_name;
	}

	public void setLocationName(String location_name) {
		this.location_name = location_name;
	}

	public Date getPublicationDate() {
		return this.publicationDate;
	}

	public void setPublicationDate(Date publicationDate) {
		this.publicationDate = publicationDate;
	}

	public String getDescription() {
		return this.description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getUrl() {
		return this.url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getType() {
		return this.type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public List<Source> getSources() {
		return this.sources;
	}

	public void setSources(List<Source> sources) {
		this.sources = sources;
	}

	public List<Project> getProjects() {
		return this.projects;
	}

	public void setProjects(List<Project> projects) {
		this.projects = projects;
	}

	public List<PublicationIdentifier> getPublicationIdentifiers() {
		return this.publicationIdentifiers;
	}

	public void setPublicationIdentifiers(List<PublicationIdentifier> publicationIdentifiers) {
		this.publicationIdentifiers = publicationIdentifiers;
	}

	public List<Person> getAuthors() {
		return this.authors;
	}

	public void setAuthors(List<Person> authors) {
		this.authors = authors;
	}

	public List<Thematic> getThematics() {
		return this.thematics;
	}

	public void setThematics(List<Thematic> thematics) {
		this.thematics = thematics;
	}
}
