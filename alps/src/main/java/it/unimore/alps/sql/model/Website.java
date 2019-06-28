package it.unimore.alps.sql.model;

import java.io.Serializable;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQuery;

import org.eclipse.persistence.annotations.CascadeOnDelete;

/**
 * The persistent class for the organization database table.
 * 
 */
@CascadeOnDelete
@Entity
@NamedQuery(name = "Website.findAll", query = "SELECT w FROM Website w")
public class Website implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private int id;

	private String url;

	private String correctUrl;
	
	
	public Website() {
	}

	public int getId() {
		return this.id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getCorrectUrl() {
		return correctUrl;
	}

	public void setCorrectUrl(String correctUrl) {
		this.correctUrl = correctUrl;
	}
	
}