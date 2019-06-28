package it.unimore.alps;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import javax.persistence.criteria.CriteriaBuilder;

import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationType;

public class TestLabel {

	public static void main(String[] args) {
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory("alps");

		EntityManager entitymanager = emfactory.createEntityManager();
		CriteriaBuilder criteriaBuilder = entitymanager.getCriteriaBuilder();

		String[] types = { "TIPO1", "TIPO2", "TIPO2" };
		for (int i = 0; i < 3; i++) {
			entitymanager.getTransaction().begin();
			Organization org = new Organization();
//			org.setAcronym("TEST" + i);
			org.setLabel("TEST Corporation " + i);

			String label = types[i];

			OrganizationType type = new OrganizationType();
			// type.setId(666);
			type.setLabel(label);

			// OrganizationType resType = entitymanager.find(OrganizationType.class, 666);

			Query query = entitymanager.createQuery("Select e FROM OrganizationType e WHERE e.label = :label");
			query.setParameter("label", label);
			List<OrganizationType> results = query.getResultList();
			System.out.println("******************");
			for (OrganizationType res : results) {
				System.out.println(res.getLabel());
			}
			System.out.println("******************");
			if (results.size() > 0) {
				if (results.get(0) != null) {
					type = results.get(0);
					System.out.println("ESISTE");
				}
			} else {
				// orgType = new OrganizationType();
				// orgType.setLabel(type);
				System.out.println("NON ESISTE");
				entitymanager.persist(type);
			}
			org.setOrganizationType(type);
			//

			entitymanager.persist(org);

			entitymanager.getTransaction().commit();
		}
		entitymanager.close();
		emfactory.close();

	}

}
