package it.unimore.alps;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import javax.persistence.criteria.CriteriaBuilder;

import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationIdentifier;
import it.unimore.alps.sql.model.OrganizationType;
import it.unimore.alps.sql.model.Project;

public class TestDataInsertion {

	public static void main(String[] args) {
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory("alps");

		EntityManager entitymanager = emfactory.createEntityManager();
		CriteriaBuilder criteriaBuilder = entitymanager.getCriteriaBuilder();
		entitymanager.getTransaction().begin();

		Organization org = new Organization();
//		org.setAcronym("XXX");
		org.setLabel("Youporn Corporation");

		OrganizationType type = new OrganizationType();
		type.setId(666);
		type.setLabel("Mister pickle");

		OrganizationType resType = entitymanager.find(OrganizationType.class, 666);
		if (resType != null) {
			System.out.println("NOT NULL");
			type = resType;
		} else {
			System.out.println("NULL");
			entitymanager.persist(type);
		}

		// entitymanager.persist(type);

		org.setOrganizationType(type);
		//
		OrganizationIdentifier identifier = new OrganizationIdentifier();
		// identifier.setId(1);
		identifier.setIdentifier("ffff");
		// identifier.setOrganization(porno);
		identifier.setProvenance("openass");
		entitymanager.persist(identifier);

		// porno.addOrganizationIdentifier(identifier);
		List<OrganizationIdentifier> organizationIdentifiers = new ArrayList<>();
		organizationIdentifiers.add(identifier);
		org.setOrganizationIdentifiers(organizationIdentifiers);

		Project p = new Project();
		p.setAcronym("test");
		p.setBudget("");

		entitymanager.persist(p);

		Query query = entitymanager.createQuery("Select p " + "from Project p ");
		List<Project> projs = query.getResultList();
		int id = query.getFirstResult();
		// System.out.println("ID: " + id);
		List<Project> projects = new ArrayList<>();
		System.out.println("\nPROJs:\n");
		for (Project proj : projs) {
			System.out.println("ID: " + proj.getId());
			projects.add(proj);
		}

		System.out.println("\n\n");
		// List<Employee> list = (List<Employee>) query.getResultList();

		org.setProjects(projects);
		entitymanager.persist(org);

		entitymanager.getTransaction().commit();

		entitymanager.close();
		emfactory.close();
	}

}
