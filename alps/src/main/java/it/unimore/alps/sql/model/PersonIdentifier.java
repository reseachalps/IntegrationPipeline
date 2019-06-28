package it.unimore.alps.sql.model;

import java.io.Serializable;
import javax.persistence.*;

import org.eclipse.persistence.annotations.CascadeOnDelete;


/**
 * The persistent class for the person_identifiers database table.
 * 
 */
@CascadeOnDelete
@Entity
@Table(name="person_identifiers")
@NamedQuery(name="PersonIdentifier.findAll", query="SELECT o FROM PersonIdentifier o")
public class PersonIdentifier implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy=GenerationType.SEQUENCE)
	private int id;
	
	private String identifier_name;

	@Column(columnDefinition = "TEXT")
	private String identifier_value;

	private String provenance;

	//bi-directional many-to-one association to Organization
	@ManyToOne(cascade = { CascadeType.PERSIST, CascadeType.REMOVE })
	private Person person;

	
	@Column(name = "visibility")
	private boolean visibility = true;
	
	public PersonIdentifier() {
	}
	
	public int getId() {
		return this.id;
	}

	public void setId(int id) {
		this.id = id;
	}	
	
	public String getIdentifierName() {
		return this.identifier_name;
	}

	public void setIdentifierName(String identifierName) {
		this.identifier_name = identifierName;
	}

	public String getIdentifier() {
		return this.identifier_value;
	}

	public void setIdentifier(String identifier) {
		this.identifier_value = identifier;
	}

	public String getProvenance() {
		return this.provenance;
	}

	public void setProvenance(String provenance) {
		this.provenance = provenance;
	}

	public Person getPerson() {
		return this.person;
	}

	public void setPerson(Person person) {
		this.person = person;
	}
	
	

	public boolean isVisibility() {
		return visibility;
	}

	public void setVisibility(boolean visibility) {
		this.visibility = visibility;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((identifier_name == null) ? 0 : identifier_name.hashCode());
		result = prime * result + ((identifier_value == null) ? 0 : identifier_value.hashCode());
		result = prime * result + ((provenance == null) ? 0 : provenance.hashCode());
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
		PersonIdentifier other = (PersonIdentifier) obj;
		if (identifier_name == null) {
			if (other.identifier_name != null)
				return false;
		} else if (!identifier_name.equals(other.identifier_name))
			return false;
		if (identifier_value == null) {
			if (other.identifier_value != null)
				return false;
		} else if (!identifier_value.equals(other.identifier_value))
			return false;
		if (provenance == null) {
			if (other.provenance != null)
				return false;
		} else if (!provenance.equals(other.provenance))
			return false;
		return true;
	}
	
	
	
	
	

}