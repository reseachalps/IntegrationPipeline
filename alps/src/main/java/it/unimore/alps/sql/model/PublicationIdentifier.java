package it.unimore.alps.sql.model;

import java.io.Serializable;
import javax.persistence.*;

import org.eclipse.persistence.annotations.CascadeOnDelete;


/**
 * The persistent class for the publication_identifiers database table.
 * 
 */
@CascadeOnDelete
@Entity
@Table(name="publication_identifiers")
@NamedQuery(name="PublicationIdentifier.findAll", query="SELECT o FROM PublicationIdentifier o")
public class PublicationIdentifier implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy=GenerationType.SEQUENCE)
	private int id;
	
	private String identifierName;

	private String identifier;

	private String provenance;

	//bi-directional many-to-one association to Organization
	@ManyToOne(cascade = { CascadeType.PERSIST, CascadeType.REMOVE })
	private Publication publication;

	
	@Column(name = "visibility")
	private boolean visibility = true;
	
	
	public PublicationIdentifier() {
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

	public Publication getPublication() {
		return this.publication;
	}

	public void setPublication(Publication publication) {
		this.publication = publication;
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
		result = prime * result + ((identifier == null) ? 0 : identifier.hashCode());
		result = prime * result + ((identifierName == null) ? 0 : identifierName.hashCode());
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
		PublicationIdentifier other = (PublicationIdentifier) obj;
		if (identifier == null) {
			if (other.identifier != null)
				return false;
		} else if (!identifier.equals(other.identifier))
			return false;
		if (identifierName == null) {
			if (other.identifierName != null)
				return false;
		} else if (!identifierName.equals(other.identifierName))
			return false;
		if (provenance == null) {
			if (other.provenance != null)
				return false;
		} else if (!provenance.equals(other.provenance))
			return false;
		return true;
	}
	
	
	

}
