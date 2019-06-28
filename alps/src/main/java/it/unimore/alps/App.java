package it.unimore.alps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationIdentifier;
import it.unimore.alps.sql.model.OrganizationType;

import it.unimore.alps.sources.arianna.*;
import it.unimore.alps.sources.cercauniversita.CercaUniversitaImporter;
import it.unimore.alps.sources.questio.*;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {

		// QUESTIO IMPORTER INVOCATION ------------------------------
		// String csvFile =
		// "/media/matteo/Windows/Users/matte/Dropbox/Matteo/Documenti/Studio/Progetto_RESEARCH_ALPS/Dataset/QUESTIO/QuestioCrawledData.csv";
		// QuestioImporter questioImporter = new QuestioImporter(csvFile, true);
		// questioImporter.importData();

		// ARIANNA IMPORTER INVOCATION ------------------------------
		// String orgFile =
		// "/media/matteo/Windows/Users/matte/Dropbox/Matteo/Documenti/Studio/Progetto_RESEARCH_ALPS/Dataset/Arianna/arianna1_soggetti_01dic17.csv";
		// String projectFile =
		// "/media/matteo/Windows/Users/matte/Dropbox/Matteo/Documenti/Studio/Progetto_RESEARCH_ALPS/Dataset/Arianna/arianna_progetti_01dic17.csv";
		// String bindingFile =
		// "/media/matteo/Windows/Users/matte/Dropbox/Matteo/Documenti/Studio/Progetto_RESEARCH_ALPS/Dataset/Arianna/raccordo
		// soggetti progetti_19dic2017.csv";
		// AriannaImporter ariannaImporter = new AriannaImporter(orgFile, projectFile,
		// bindingFile, true);
		// ariannaImporter.importData("all");
		// ariannaImporter.importData("organization");
		// ariannaImporter.importData("project");
		// ariannaImporter.importData("link");

		// CERCAUNIVERSITA IMPORTER INVOCATION ----------------------
		// String universityFile = "/home/matteo/Scaricati/Atenei.csv";
		// String departmentFile = "/home/matteo/Scaricati/Dipartimenti.csv";
		// String instituteFile = "/home/matteo/Scaricati/Istituti.csv";
		// String centreFile = "/home/matteo/Scaricati/Centri.csv";
		// CercaUniversitaImporter cercauniversitaImporter = new
		// CercaUniversitaImporter(universityFile, departmentFile, instituteFile,
		// centreFile, true);
		// cercauniversitaImporter.importData();

		String s = "corda__h2020::9c4c76c8fc5d5041a0b898cd9d7f4316";
		if(s.contains("::")) {
			System.out.println(s.substring(s.indexOf("::")+2));
		}
			
		System.out.println();
		
		
	}
}
