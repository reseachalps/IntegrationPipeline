package it.unimore.alps.sql.model;

import java.io.Serializable;
import javax.persistence.*;

import org.eclipse.persistence.annotations.CascadeOnDelete;

/**
 * The persistent class for the thematic database table.
 *
 */
@CascadeOnDelete
@Entity
@NamedQuery(name = "Thematic.findAll", query = "SELECT a FROM Thematic a")
public class Thematic implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE)
	private int id;
	
	private String code;

	private String label;

	private String classificationSystem;

	public Thematic() {
	}
	
	public int getId() {
		return this.id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getCode() {
		return this.code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getLabel() {
		return this.label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public String getClassificationSystem() {
		return classificationSystem;
	}

	public void setClassificationSystem(String classificationSystem) {
		this.classificationSystem = classificationSystem;
	}

}
