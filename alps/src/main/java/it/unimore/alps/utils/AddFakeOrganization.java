package it.unimore.alps.utils;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import it.unimore.alps.sql.model.Organization;

public class AddFakeOrganization {

	public static void main(String[] args) {
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory("alpsDedup");
		EntityManager entitymanager = emfactory.createEntityManager();

		entitymanager.getTransaction().begin();

		Organization org = new Organization();
		org.setLabel("FAKE");
		entitymanager.persist(org);

		System.out.println("Inserted fake organization");

		entitymanager.getTransaction().commit();

		entitymanager.close();
		emfactory.close();

	}

}
