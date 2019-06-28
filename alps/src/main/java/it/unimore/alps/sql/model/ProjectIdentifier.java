package it.unimore.alps.sql.model;

import java.io.Serializable;
import javax.persistence.*;

import org.eclipse.persistence.annotations.CascadeOnDelete;


/**
 * The persistent class for the project_identifiers database table.
 * 
 */
@CascadeOnDelete
@Entity
@Table(name="project_identifiers")
@NamedQuery(name="ProjectIdentifier.findAll", query="SELECT p FROM ProjectIdentifier p")
public class ProjectIdentifier implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy=GenerationType.SEQUENCE)
	private int id;	
	
	private String identifierName;

	@Column(columnDefinition = "TEXT")
	private String identifier;

	private String provenance;

	//bi-directional many-to-one association to Project
	@ManyToOne(cascade = { CascadeType.PERSIST, CascadeType.REMOVE })
	private Project project;
	
	@Column(name = "visibility")
	private boolean visibility = true;

	public ProjectIdentifier() {
	}

	public int getId() {
		return this.id;
	}

	public void setId(int id) {
		this.id = id;
	}	
	
	public String getIdentifierName() {
		return this.identifierName;
	}

	public void setIdentifierName(String identifierName) {
		this.identifierName = identifierName;
	}

	public String getIdentifier() {
		return this.identifier;
	}

	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	public String getProvenance() {
		return this.provenance;
	}

	public void setProvenance(String provenance) {
		this.provenance = provenance;
	}

	public Project getProject() {
		return this.project;
	}

	public void setProject(Project project) {
		this.project = project;
	}

	public boolean isVisibility() {
		return visibility;
	}

	public void setVisibility(boolean visibility) {
		this.visibility = visibility;
	}

	
	
}