package it.unimore.alps.sql.model;

import java.io.Serializable;
import java.util.List;

import javax.persistence.*;

import org.eclipse.persistence.annotations.CascadeOnDelete;

/**
 * The persistent class for the person database table.
 * 
 */
@CascadeOnDelete
@Entity
@NamedQuery(name = "Person.findAll", query = "SELECT l FROM Person l")
public class Person implements Serializable {
	private static final long serialVersionUID = 1L;

	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE)
	private int id;

	private String firstname;

	private String lastname;

	private String title;

	private String email;

	// bi-directional many-to-one association to PersonIdentifier
	@OneToMany(mappedBy = "person", cascade = { CascadeType.PERSIST, CascadeType.REMOVE }, orphanRemoval = true)
	private List<PersonIdentifier> personIdentifiers;

	// uni-directional many-to-many association to Source
	@ManyToMany(cascade = { CascadeType.PERSIST, CascadeType.REMOVE })
	@JoinTable(name = "person_sources", joinColumns = { @JoinColumn(name = "person_id") }, inverseJoinColumns = {
			@JoinColumn(name = "source_id") })
	private List<Source> sources;

	public Person() {
	}

	public int getId() {
		return this.id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getFirstName() {
		return this.firstname;
	}

	public void setFirstName(String firstName) {
		this.firstname = firstName;
	}

	public String getLastName() {
		return this.lastname;
	}

	public void setLastName(String lastName) {
		this.lastname = lastName;
	}

	public String getTitle() {
		return this.title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getEmail() {
		return this.email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public List<PersonIdentifier> getPersonIdentifiers() {
		return this.personIdentifiers;
	}

	public void setPersonIdentifiers(List<PersonIdentifier> personIdentifiers) {
		this.personIdentifiers = personIdentifiers;
	}

	public List<Source> getSources() {
		return this.sources;
	}

	public void setSources(List<Source> sources) {
		this.sources = sources;
	}

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
		Person other = (Person) obj;
		if (id != other.id)
			return false;
		return true;
	}

	

	

}
