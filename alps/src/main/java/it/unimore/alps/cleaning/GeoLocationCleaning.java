package it.unimore.alps.cleaning;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import it.unimore.alps.cleaning.GeoLocationUtil.LocationData;
import it.unimore.alps.integrator.GeoIntegratorOld;
import it.unimore.alps.sql.model.Organization;

public class GeoLocationCleaning {

	public static void main(String[] args) {

		// String db = "alpsv7";

		CommandLine commandLine;

		//

		Option dbOption = Option.builder("db").hasArg().required(true)
				.desc("The name of the database of the data ingestion.").longOpt("DB").build();

		Options options = new Options();

		options.addOption(dbOption);

		CommandLineParser parser = new DefaultParser();

		String db = null;

		try {
			commandLine = parser.parse(options, args);

			if (commandLine.hasOption("db")) {
				db = commandLine.getOptionValue("db");
			} else {
				System.out.println("Destination DB is not provided. Use the db option.");
				System.exit(0);
			}
			System.out.println("db: " + db);

		} catch (org.apache.commons.cli.ParseException e) {

			e.printStackTrace();
		}

		GeoIntegratorOld integrator = new GeoIntegratorOld();

		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();

		List<Organization> orgs = integrator.retrieveOrganizationWithIds(entitymanager, db);

		GeoLocationUtil util = new GeoLocationUtil();
		util.loadDatabases();

		int count = 0;
		int countExtraFix = 0;
		entitymanager.getTransaction().begin();
		for (Organization org : orgs) {

			if (org.getCountryCode() == null) {
				System.out.println("Null CountryCode for organization: " + org.getLabel());
				continue;
			}

			if (org.getCountryCode().toLowerCase().equals("fr")) {
				continue;
			}

			LocationData data = util.checkOrganizationLocation(org);

			if (data != null) {
				String city = data.city;
				String urbanUnit = data.urbanUnit;
				String urbanUnitCode = data.urbanUnitCode;

				org.setCity(city);
				org.setUrbanUnit(urbanUnit);
				org.setUrbanUnitCode(urbanUnitCode);

				entitymanager.merge(org);
				count++;

			} else {

				String urban_code = org.getUrbanUnit();
				if (urban_code != null) {
					if (urban_code.length() == 2) {
						LocationData dataReduced = util.checkOrganizationLocationByProvince(org, urban_code);
						if (dataReduced != null) {
							String urbanUnit = dataReduced.urbanUnit;
							String urbanUnitCode = dataReduced.urbanUnitCode;
							org.setUrbanUnit(urbanUnit);
							org.setUrbanUnitCode(urbanUnitCode);
							countExtraFix++;
							entitymanager.merge(org);
						}
					}
				}

			}

		}
		entitymanager.getTransaction().commit();
		entitymanager.close();
		emfactory.close();

		System.out.println("Update done!!! Number of organization updated: " + count);
		System.out.println("Extra updates: " + countExtraFix);

	}

}
