package it.unimore.alps.idgenerator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import it.unimore.alps.sql.model.Link;
import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationIdentifier;
import it.unimore.alps.sql.model.Person;
import it.unimore.alps.sql.model.PersonIdentifier;
import it.unimore.alps.sql.model.Project;
import it.unimore.alps.sql.model.ProjectIdentifier;
import it.unimore.alps.sql.model.Source;

public class LocalIdentifierGenerator {
	
	public static void main(String[] args) {
		
		
		CommandLine commandLine;
        Option sourceNameOption = Option.builder("sourceName").hasArg().required(true).desc("The source name for which to test unique identifier.").longOpt("sourceName").build();
        Option DB = Option.builder("DB").hasArg().required(true).desc("DB. ").longOpt("DB").build();

        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addOption(sourceNameOption);
        options.addOption(DB);
        
        String sourceName = null;
        String db = null;
                	
        try {
			commandLine = parser.parse(options, args);
						
			System.out.println("----------------------------");
			System.out.println("OPTIONS:");

	        if (commandLine.hasOption("sourceName")) {   
	        	sourceName = commandLine.getOptionValue("sourceName");
	        	System.out.println("\tSource name: " + sourceName);
			} else {
				System.out.println("\tSource name not provided. Use the sourceName option.");
	        	System.exit(1);
			}  	
	        
	        if (commandLine.hasOption("DB")) {
				db = commandLine.getOptionValue("DB");
				System.out.println("\tDatabase: " + db);
			} else {
				System.out.println("\tSource database is not provided. Use the DB option.");
				System.exit(1);
			}			
			
			System.out.println("----------------------------\n");
        
        } catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();
		
		Source source = getSourceByName(entitymanager, sourceName);
		List<Organization> orgs = retrieveOrganizations(entitymanager, source);
		Set<String> identifiers = new HashSet<>();	
		boolean first = true;
		
	    switch (sourceName) {
		    case "ORCID":		    	
		    	for (Organization org: orgs) {
		    		
		    		//String identifier = sourceName + "::"+org.getCountry()+"::"+org.getCity()+"::"+org.getUrbanUnit()+"::"+org.getAddress()+"::"+org.getLabel()+"::"+org.getPostcode()+"::"+getOrgIds(org)+"::"+getLinks(org)+"::"+getChildren(org)+":"+getPeopleIdentifiers(org);
		    		String identifier =  sourceName + "::" + org.getLabel() + "::" + getPeopleIdentifiers(org);
		    		if (first) {
		    			System.out.println("Identifier template example: " + identifier);
		    			first = false;
		    		}
		    		//int prec_size = identifiers.size(); 
		    		identifiers.add(identifier);
		    		/*if (prec_size == identifiers.size()) {
		    			System.out.println(org.getId() + " " + identifier);
		    		}*/
		    		
		    	}
		    	break;
		    case "ScanR":
		    	for (Organization org: orgs) {
		    		//String identifier = sourceName + "::"+org.getCountry()+"::"+org.getCity()+"::"+org.getAddress()+"::"+org.getLabel()+"::"+org.getPostcode();
		    		String identifier = sourceName + "::" + getOrgIds(org);
		    		if (first) {
		    			System.out.println("Identifier template example: " + identifier);
		    			first = false;
		    		}
		    		identifiers.add(identifier);
		    	}
		    	break;
		    case "Arianna - Anagrafe Nazionale delle Ricerche":
		    	for (Organization org: orgs) {
		    		String identifier = sourceName + "::" + getOrgIds(org);
		    		if (first) {
		    			System.out.println("Identifier template example: " + identifier);
		    			first = false;
		    		}
		    		identifiers.add(identifier);
		    	}
		    	break;
		    case "Crawled":
		    	for (Organization org: orgs) {
		    		String identifier = sourceName + "::" + org.getLabel() +"::" + getOrgIds(org);
		    		if (first) {
		    			System.out.println("Identifier template example: " + identifier);
		    			first = false;
		    		}
		    		identifiers.add(identifier);
		    	}
		    	break;
		    case "CercaUniversita":
		    	for (Organization org: orgs) {
		    		String identifier = sourceName + "::" + org.getLabel() + "::" + org.getAddress() + "::" + org.getCity();
		    		if (first) {
		    			System.out.println("Identifier template example: " + identifier);
		    			first = false;
		    		}
		    		identifiers.add(identifier);
		    	}
		    	break;
		    case "Consiglio Nazionale delle Ricerche (CNR)":
		    	for (Organization org: orgs) {
		    		String identifier = sourceName + "::" + org.getLabel() + "::" + org.getAddress();
		    		if (first) {
		    			System.out.println("Identifier template example: " + identifier);
		    			first = false;
		    		}
		    		identifiers.add(identifier);
		    	}
		    	break;
		    case "Patiris":
		    	for (Organization org: orgs) {
		    		String identifier = sourceName + "::" + org.getLabel();
		    		if (first) {
		    			System.out.println("Identifier template example: " + identifier);
		    			first = false;
		    		}
		    		identifiers.add(identifier);
		    	}
		    	break;
		    case "Questio":
		    	for (Organization org: orgs) {
		    		String identifier = sourceName + "::" + org.getLabel() + "::" + getLinks(org) + "::" + org.getCity() + "::" + org.getProjects().size();
		    		if (first) {
		    			System.out.println("Identifier template example: " + identifier);
		    			first = false;
		    		}
		    		int prec_size = identifiers.size();
		    		identifiers.add(identifier);
		    		if (prec_size == identifiers.size()) {
		    			System.out.println(org.getId() + " " + identifier);
		    		}
		    	}
		    	break;
		    case "Startup - Registro delle imprese":
		    	for (Organization org: orgs) {
		    		String identifier = sourceName + "::" + getOrgIds(org);
		    		if (first) {
		    			System.out.println("Identifier template example: " + identifier);
		    			first = false;
		    		}
		    		identifiers.add(identifier);
		    	}
		    	break;
		    case "Austrian Science Fund (FWF)":
		    	for (Organization org: orgs) {
		    		String identifier = sourceName + "::" + org.getLabel() + "::" + getOrgProjectIdentifiers(org);// + "::" + getChildren(org);
		    		if (first) {
		    			System.out.println("Identifier template example: " + identifier);
		    			first = false;
		    		}
		    		int prec_size = identifiers.size(); 
		    		identifiers.add(identifier);
		    		if (prec_size == identifiers.size()) {
		    			System.out.println(org.getId() + " " + identifier);
		    		}
		    	}
		    	break;
		    case "P3":
		    	for (Organization org: orgs) {
		    		String identifier = sourceName + "::" + org.getLabel() + "::" + getOrgProjectIdentifiers(org);
		    		if (first) {
		    			System.out.println("Identifier template example: " + identifier);
		    			first = false;
		    		}
		    		identifiers.add(identifier);
		    	}
		    	break;
		    case "Grid":
		    	for (Organization org: orgs) {
		    		String identifier = sourceName + "::" + getOrgIds(org);
		    		if (first) {
		    			System.out.println("Identifier template example: " + identifier);
		    			first = false;
		    		}
		    		identifiers.add(identifier);
		    	}
		    	break;
		    case "OpenAire":
		    	for (Organization org: orgs) {
		    		String identifier = sourceName + "::" + getOrgIds(org);
		    		if (first) {
		    			System.out.println("Identifier template example: " + identifier);
		    			first = false;
		    		}
		    		identifiers.add(identifier);
		    	}
		    	break;
	    }
	    
	    System.out.println();
	    System.out.println("---------------------------");
	    System.out.println("\tNumero organizzazioni: " + orgs.size());
	    System.out.println("\tNumero identifiers: " + identifiers.size());
	    if (identifiers.size() != orgs.size()) {
	    	System.out.println("\tThe selected identifier template is not unique.");
	    } else {
	    	System.out.println("\tThe selected identifier template is unique.");
	    }
	    System.out.println("---------------------------");
	}
	
	public static List<Organization> retrieveOrganizations(EntityManager entitymanager, Source source) {

		entitymanager.getTransaction().begin();
		Query query = entitymanager.createQuery("Select o FROM Organization o WHERE :id MEMBER OF o.sources");
		query.setParameter("id", source);
		List<Organization> orgs = query.getResultList();

		entitymanager.getTransaction().commit();
		System.out.println("Retrieved " + orgs.size() + " organizations");

		return orgs;
	}
	
	public static Source getSourceByName(EntityManager entitymanager, String sourceName) {
		Query query = entitymanager.createQuery("Select s FROM Source s where s.label=:sourceLabel");
		query.setParameter("sourceLabel", sourceName);
		List<Source> sources = query.getResultList();
		Source source = sources.get(0);
		return source;
	}
	
	public static String getLinks(Organization org) {
		List<Link> links = org.getLinks();
		String linkId = null;
		
		if (links != null) {
			List<String> linksNames = new ArrayList<>();
			for (Link link: links) {
				linksNames.add(link.getUrl());
			}
			linkId = String.join("_", linksNames);
		}
		
		return linkId;
	}
	
	public static String getChildren(Organization org) {
		List<Organization> childOrgs = org.getChildrenOrganizations();
		String orgId = null;
		
		if (childOrgs != null) {
			List<String> orgsNames = new ArrayList<>();
			for (Organization o: childOrgs) {
				orgsNames.add(o.getLabel());
			}
			orgId = String.join("_", orgsNames);
		}
		
		return orgId;
	}
	
	public static String getOrgIds(Organization org) {
		List<OrganizationIdentifier> orgIds = org.getOrganizationIdentifiers();
		String orgId = null;
		
		if (orgIds != null) {
			List<String> ids = new ArrayList<>();
			for (OrganizationIdentifier oi: orgIds) {
				ids.add(oi.getIdentifierName() + "_" + oi.getIdentifier());
			}
			orgId = String.join(",", ids);
		}
		
		return orgId;
	}
	
	public static String getPeopleIdentifiers(Organization org) {
		List<Person> people = org.getPeople();
		String orgId = null;
		
		if (people != null) {
			List<String> ids = new ArrayList<>();
			for (Person p: people) {
				ids.add(p.getFirstName() + " " + p.getLastName());
				List<PersonIdentifier> pIds = p.getPersonIdentifiers();
				if (pIds != null) {
					for(PersonIdentifier pi: pIds) {
						ids.add(pi.getIdentifierName() + " " + pi.getIdentifier());
					}
				}
			}
			orgId = String.join(",", ids);
		}
		
		return orgId;
	}
	
	public static String getOrgProjectIdentifiers(Organization org) {
		List<Project> prjs = org.getProjects();
		String orgId = null;
		
		if (prjs != null) {
			List<String> ids = new ArrayList<>();
			for (Project p: prjs) {
				List<ProjectIdentifier> pIds = p.getProjectIdentifiers();
				if (pIds != null) {
					for(ProjectIdentifier pi: pIds) {
						ids.add(pi.getIdentifierName() + " " + pi.getIdentifier());
					}
				}
			}
			orgId = String.join(",", ids);
		}
		
		return orgId;
	}

}
