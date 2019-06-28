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
@NamedQuery(name = "ExternalParticipant.findAll", query = "SELECT a FROM ExternalParticipant a")
public class ExternalParticipant implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8348073888509028562L;

	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE)
	private int id;

	public ExternalParticipant() {
	}

	private String label;

	private String url;

	public int getId() {
		return this.id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return label + "\t" + url;

	}

}
