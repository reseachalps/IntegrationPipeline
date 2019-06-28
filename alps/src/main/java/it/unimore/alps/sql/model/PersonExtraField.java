package it.unimore.alps.sql.model;

import java.io.Serializable;
import javax.persistence.*;

import org.eclipse.persistence.annotations.CascadeOnDelete;


/**
 * The persistent class for the person_extra_field database table.
 * 
 */
@CascadeOnDelete
@Entity
@Table(name="person_extra_field")
@NamedQuery(name="PersonExtraField.findAll", query="SELECT o FROM PersonExtraField o")
public class PersonExtraField implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy=GenerationType.SEQUENCE)
	@Column(name="field_id")
	private int fieldId;

	@Column(name="field_key")
	private String fieldKey;

	@Column(name="field_value")
	private String fieldValue;

	//bi-directional many-to-one association to Person
	@ManyToOne(cascade = { CascadeType.PERSIST, CascadeType.REMOVE })
	private Person person;
	
	@Column(name = "visibility")
	private boolean visibility = false;

	public PersonExtraField() {
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
	
	

}
