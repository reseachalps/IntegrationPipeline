package it.unimore.alps.sql.model;

import java.io.Serializable;
import javax.persistence.*;

import org.eclipse.persistence.annotations.CascadeOnDelete;


/**
 * The persistent class for the organization_type database table.
 * 
 */
@CascadeOnDelete
@Entity
@Table(name="organization_type")
@NamedQuery(name="OrganizationType.findAll", query="SELECT o FROM OrganizationType o")
public class OrganizationType implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy=GenerationType.SEQUENCE)
	private int id;

	private String label;

	public OrganizationType() {
	}

	public int getId() {
		return this.id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getLabel() {
		return this.label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

}