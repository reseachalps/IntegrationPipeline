package it.unimore.alps.sql.model;

import java.io.Serializable;
import javax.persistence.*;

import org.eclipse.persistence.annotations.CascadeOnDelete;

/**
 * The persistent class for the organization_extra_field database table.
 * 
 */
@CascadeOnDelete
@Entity
@Table(name = "organization_extra_field")
@NamedQuery(name = "OrganizationExtraField.findAll", query = "SELECT o FROM OrganizationExtraField o")
public class OrganizationExtraField implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE)
	@Column(name = "field_id")
	private int fieldId;

	@Column(name = "field_key")
	private String fieldKey;

	@Column(name = "field_value")
	private String fieldValue;

	// bi-directional many-to-one association to Organization
	@ManyToOne(cascade = { CascadeType.PERSIST, CascadeType.REMOVE })
	private Organization organization;

	@Column(name = "visibility")
	private boolean visibility = false;

	public OrganizationExtraField() {
	}

	public int getFieldId() {
		return this.fieldId;
	}

	public void setFieldId(int fieldId) {
		this.fieldId = fieldId;
	}

	public String getFieldKey() {
		return this.fieldKey;
	}

	public void setFieldKey(String fieldKey) {
		this.fieldKey = fieldKey;
	}

	public String getFieldValue() {
		return this.fieldValue;
	}

	public void setFieldValue(String fieldValue) {
		this.fieldValue = fieldValue;
	}

	public Organization getOrganization() {
		return this.organization;
	}

	public void setOrganization(Organization organization) {
		this.organization = organization;
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
		result = prime * result + ((fieldKey == null) ? 0 : fieldKey.hashCode());
		result = prime * result + ((fieldValue == null) ? 0 : fieldValue.hashCode());
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
		OrganizationExtraField other = (OrganizationExtraField) obj;
		if (fieldKey == null) {
			if (other.fieldKey != null)
				return false;
		} else if (!fieldKey.equals(other.fieldKey))
			return false;
		if (fieldValue == null) {
			if (other.fieldValue != null)
				return false;
		} else if (!fieldValue.equals(other.fieldValue))
			return false;
		return true;
	}

}