package it.unimore.alps.sources.patiris;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import it.unimore.alps.sql.model.OrganizationExtraField;
import it.unimore.alps.sql.model.OrganizationIdentifier;
import it.unimore.alps.sql.model.Person;
import it.unimore.alps.sql.model.Publication;
import it.unimore.alps.sql.model.PublicationIdentifier;
import it.unimore.alps.sql.model.Source;
import it.unimore.alps.sql.model.Thematic;

public class PatirisImporter {

	private String sourceName = "Patiris";								// data source name
	private String sourceUrl = "http://patiris.uibm.gov.it/home";		// data source url
	private String sourceRevisionDate = "01-03-2018";					// data source date

	public static void main(String[] args) {

		// BEGIN: INPUT PARAMETERS ------------------------------------------------------
		CommandLine commandLine;
		// database where to import data
		Option DB = Option.builder("DB").hasArg().required(true).desc("DB. ").longOpt("DB").build();
		
		Options options = new Options();
		CommandLineParser parser = new DefaultParser();
		
		options.addOption(DB);
		
		String db = null;
		
		try {
			commandLine = parser.parse(options, args);

			System.out.println("----------------------------");
			System.out.println("OPTIONS:");

			if(commandLine.hasOption("DB")) {       	
	        	db =commandLine.getOptionValue("DB");
	        	System.out.println("DB name: " + db);
	        } else {
	        	System.out.println("\tDB name not provided. Use the DB option.");
	        	System.exit(1);
	        }
		} catch (org.apache.commons.cli.ParseException e) {
			e.printStackTrace();
		}
		// END: INPUT PARAMETERS --------------------------------------------------------
		
		// import data
		PatirisImporter patirisImporter = new PatirisImporter();
		patirisImporter.importData(db);

	}

	public PatirisImporter() {
	}

	private List<PublicationIdentifier> setPublicationIdentifiers(Object[] data, Publication publication,
			Long patentId) {

		// save publication identifier information into PublicationIdentifier object
		List<PublicationIdentifier> pubIds = new ArrayList<PublicationIdentifier>();

		PublicationIdentifier pubId = new PublicationIdentifier();

		pubId.setIdentifier("" + patentId);
		pubId.setProvenance(sourceName);
		pubId.setIdentifierName("id");
		pubId.setPublication(publication);
		pubIds.add(pubId);

		return pubIds;

	}
	
	/*private List<PublicationExtraField> setPublicationExtraFields(Object[] o, Publication pub) {
		
		List<PublicationExtraField> pubExtraFields = new ArrayList<PublicationExtraField>();		
		
		Date applicationDate = (Date) o[1];
		if (applicationDate != null) {
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String applicationDateString = df.format(applicationDate);
			OrganizationExtraField orgExtraField = new OrganizationExtraField();
			orgExtraField.setVisibility(true);
			orgExtraField.setFieldKey("application date");
			orgExtraField.setFieldValue(applicationDateString);
			orgExtraField.setPublication(pub);
			pubExtraFields.add(orgExtraField);
		}
		
		String applicationNumber = (String) o[2];
		if (applicationNumber != null && !applicationNumber.equals("")) {
			OrganizationExtraField orgExtraField = new OrganizationExtraField();
			orgExtraField.setVisibility(true);
			orgExtraField.setFieldKey("application number");
			orgExtraField.setFieldValue(applicationNumber);
			orgExtraField.setPublication(pub);
			pubExtraFields.add(orgExtraField);
		}
				
		String kindCode = (String) o[4];
		if (kindCode != null && !kindCode.equals("")) {
			OrganizationExtraField orgExtraField = new OrganizationExtraField();
			orgExtraField.setVisibility(true);
			orgExtraField.setFieldKey("kind code");
			orgExtraField.setFieldValue(kindCode);
			orgExtraField.setPublication(pub);
			pubExtraFields.add(orgExtraField);
		}
		
		String patentAssignees = (String) o[5];
		if (patentAssignees != null && !patentAssignees.equals("")) {
			OrganizationExtraField orgExtraField = new OrganizationExtraField();
			orgExtraField.setVisibility(true);
			orgExtraField.setFieldKey("patent assignees");
			orgExtraField.setFieldValue(patentAssignees);
			orgExtraField.setPublication(pub);
			pubExtraFields.add(orgExtraField);
		}
		
		String publicationNumber = (String) o[7];
		if (publicationNumber != null && !publicationNumber.equals("")) {
			OrganizationExtraField orgExtraField = new OrganizationExtraField();
			orgExtraField.setVisibility(true);
			orgExtraField.setFieldKey("publication number");
			orgExtraField.setFieldValue(publicationNumber);
			orgExtraField.setPublication(pub);
			pubExtraFields.add(orgExtraField);
		}
		
		String publicationStage = (String) o[8];
		if (publicationStage != null && !publicationStage.equals("")) {
			OrganizationExtraField orgExtraField = new OrganizationExtraField();
			orgExtraField.setVisibility(true);
			orgExtraField.setFieldKey("publication stage");
			orgExtraField.setFieldValue(publicationStage);
			orgExtraField.setPublication(pub);
			pubExtraFields.add(orgExtraField);
		}
		
		String standardizedPatentNumber = (String) o[9];
		if (standardizedPatentNumber != null && !standardizedPatentNumber.equals("")) {
			OrganizationExtraField orgExtraField = new OrganizationExtraField();
			orgExtraField.setVisibility(true);
			orgExtraField.setFieldKey("standardized patent number");
			orgExtraField.setFieldValue(standardizedPatentNumber);
			orgExtraField.setPublication(pub);
			pubExtraFields.add(orgExtraField);
		}

		
		return pubExtraFields;
	}*/
	
	public Map<Long, Publication> importPublications(EntityManager entitymanager, List<Source> sources, List<Object[]> patents) {
		
		entitymanager.getTransaction().begin();
		
		Map<Long, Publication> pubs = new HashMap<>();
		for (Object[] p: patents) {
			
			Publication pub = new Publication();
			
			Long patentId = (Long) p[0];
			if (patentId != null) {
				List<PublicationIdentifier> publicationIdentifiers = setPublicationIdentifiers(p, pub, patentId);
        		// create PublicationIdentifier tuples in the DB
        		for (PublicationIdentifier publicationIdentifier: publicationIdentifiers) {
        			entitymanager.persist(publicationIdentifier);
        		}
			}
						
			// en_abstract attribute
			String description = (String) p[3];
			if (description != null) {
				pub.setDescription(description);
			}

			// publication_date attribute
			Date publicationDate = (Date) p[6];
			if (publicationDate != null) {
				pub.setPublicationDate(publicationDate);
			}

			// title attribute
			String patentTitle = (String) p[10];
			if (patentTitle != null) {
				pub.setTitle(patentTitle);
			}

			pub.setType("patent");
			
			// set publication extra fields
        	/*if (pub != null) {
        		List<PublicationExtraField> pubExtraFields = setPublicationExtraFields(p, pub);
        		for (OrganizationExtraField pubExtraField: pubExtraFields) {
        			entitymanager.persist(pubExtraField);
        		}
        	}*/			

			// publication sources
			pub.setSources(sources);

			if (patentId != null && pub != null) {
				pubs.put(patentId, pub);
				entitymanager.persist(pub);
			}

		}
		entitymanager.getTransaction().commit();

		return pubs;

	}

	private List<OrganizationIdentifier> setOrganizationIdentifiers(Object[] data, Organization organization,
			Long organizationId) {

		// save organization identifier information into OrganizationIdentifier object
		List<OrganizationIdentifier> orgIds = new ArrayList<OrganizationIdentifier>();

		OrganizationIdentifier orgId = new OrganizationIdentifier();

		orgId.setIdentifier("" + organizationId);
		orgId.setProvenance(sourceName);
		orgId.setIdentifierName("id");
		orgId.setOrganization(organization);
		orgIds.add(orgId);

		return orgIds;

	}

	private List<Link> setOrganizationLink(Object[] data, String url) {

		// set organization website information into Link object
		List<Link> links = new ArrayList<Link>();

		Link link = new Link();

		link.setUrl(url);
		link.setLabel("homepage");
		link.setType("main");

		links.add(link);

		return links;
	}
	
	private List<OrganizationExtraField> setOrganizationExtraFields(Object[] o, Organization org) {
		
		// save organization extra information into OrganizationExtraField objects
		List<OrganizationExtraField> orgExtraFields = new ArrayList<OrganizationExtraField>();		
		
		// fax attribute
		String fax = (String) o[5];
		if (fax != null && !fax.equals("")) {
			OrganizationExtraField orgExtraField = new OrganizationExtraField();
			orgExtraField.setVisibility(true);
			orgExtraField.setFieldKey("fax");
			orgExtraField.setFieldValue(fax);
			orgExtraField.setOrganization(org);
			orgExtraFields.add(orgExtraField);
		}

		// string_orbit attribute
		String stringOrbit = (String) o[10];
		if (stringOrbit != null && !stringOrbit.equals("")) {
			OrganizationExtraField orgExtraField = new OrganizationExtraField();
			orgExtraField.setVisibility(true);
			orgExtraField.setFieldKey("alias");
			orgExtraField.setFieldValue(stringOrbit);
			orgExtraField.setOrganization(org);
			orgExtraFields.add(orgExtraField);
		}
			
		// telefono attribute
		String phone = (String) o[11];
		if (phone != null && !phone.equals("")) {
			OrganizationExtraField orgExtraField = new OrganizationExtraField();
			orgExtraField.setVisibility(true);
			orgExtraField.setFieldKey("phone");
			orgExtraField.setFieldValue(phone);
			orgExtraField.setOrganization(org);
			orgExtraFields.add(orgExtraField);
		}
		
		return orgExtraFields;
	}
	
	
	
	public Map<Long, Organization> importOrganizations(EntityManager entitymanager, List<Source> sources, List<Object[]> orgs) {
		
		entitymanager.getTransaction().begin();
				
		Map<Long, Organization> organizations = new HashMap<>();
		
		// read organizations
		for (Object[] o: orgs) {
			
			Organization org = new Organization();
			
			Long orgId = (Long) o[0]; 
			if (orgId != null) {
				List<OrganizationIdentifier> organizationIdentifiers = setOrganizationIdentifiers(o, org, orgId);
        		// create OrganizationIdentifier tuples in the DB
        		for (OrganizationIdentifier organizationIdentifier: organizationIdentifiers) {
        			entitymanager.persist(organizationIdentifier);
        		}
			}
			
			// ateneo attribute
			String label = (String) o[1];
			if (label != null) {
				org.setLabel(label);
			}
			
			// cap attribute
			String postcode = (String) o[2];
			if (postcode != null) {
				org.setPostcode(postcode);
			}
			
			// citta attribute
			String city = (String) o[3];
			if (city != null) {
				String cleanCity = city.replaceAll("\\(.*\\)", "");
				String definitiveCleanCity = cleanCity.substring(0, 1).toUpperCase() + cleanCity.substring(1);
				org.setCity(definitiveCleanCity);
			}
			
			// sito_web attribute
			String url = (String) o[9];
			if (url != null) {			
				List<Link> links = setOrganizationLink(o, url);
				for (Link l: links) {
					l.setSources(sources);
	        		entitymanager.persist(l);
	        	}
	        	if (links.size() > 0) {
	        		org.setLinks(links);
	    		}	
			}
			
			// via attribute
			String address = (String) o[12];
			if (address != null) {
				org.setAddressSources(sources);
				org.setAddress(address);
			}
			
			org.setCountry("Italy");
			org.setCountryCode("IT");
			
			// set organization extra fields
        	if (org != null) {
        		List<OrganizationExtraField> orgExtraFields = setOrganizationExtraFields(o, org);
        		for (OrganizationExtraField orgExtraField: orgExtraFields) {
        			entitymanager.persist(orgExtraField);
        		}
        		
        		// set unique identifier
				String identifier = sourceName + "::" + org.getLabel();
				OrganizationIdentifier orgPatirisId = new OrganizationIdentifier();
				orgPatirisId.setIdentifier(identifier);
				orgPatirisId.setProvenance(sourceName);
				orgPatirisId.setIdentifierName("lid");
				orgPatirisId.setVisibility(false);
				orgPatirisId.setOrganization(org);
				entitymanager.persist(orgPatirisId);
        	}
			
			// organization sources
			org.setSources(sources);
			
			if (orgId != null && org != null) {
				organizations.put(orgId, org);
				entitymanager.persist(org);
			}
						
		}
		
		entitymanager.getTransaction().commit();
		
		return organizations;
	}
		
	public Map<Long, Thematic> importPublicationThematics(EntityManager entitymanager, List<Object[]> ipcThematics) {

		// save publication thematic information into Thematic objects
		entitymanager.getTransaction().begin();
		
		Map<Long, Thematic> thematics = new HashMap<>();
		for (Object[] t: ipcThematics) {
			
			Thematic thematic = new Thematic();						 
			
			String code = (String) t[1]; 
			if (code != null) {
				thematic.setCode(code);
			}
			
			thematic.setClassificationSystem("IPC");
			
			Long ipcId = (Long) t[0];
			if (ipcId != null && thematic != null) {
				thematics.put(ipcId, thematic);
				entitymanager.persist(thematic);
			}			
						
		}
		
		entitymanager.getTransaction().commit();
		
		return thematics;
	}
	
	public Map<Long, Person> importPeople(EntityManager entitymanager, List<Source> sources, List<Object[]> invs) {

		entitymanager.getTransaction().begin();
		
		Map<Long, Person> inventors = new HashMap<>();
		
		// read people
		for (Object[] i: invs) {
			
			Person person = new Person();						 
			
			// full_name attribute
			String fullName = (String) i[1]; 
			if (fullName != null || !fullName.equals("")) {
				// clean full name field
				fullName = fullName.replaceAll("\\(.*?\\) ?", "");
    			fullName = fullName.replace(",", "");
    			    			
				String[] fullNameItems = fullName.split(" ");
				if (fullNameItems.length > 1) {
					String firstName = fullNameItems[fullNameItems.length-1];
					String lastName = String.join(" ", Arrays.asList(fullNameItems).subList(0, fullNameItems.length-1));
					person.setFirstName(firstName);
					person.setLastName(lastName);
				} else {
					person.setLastName(fullNameItems[0]);
				}
			}
			
			// people sources
			person.setSources(sources);
			
			Long invId = (Long) i[0];
			if (invId != null && person != null) {
				inventors.put(invId, person);
				entitymanager.persist(person);
			}			
						
		}
		
		entitymanager.getTransaction().commit();
		
		return inventors;
	}
	

	private void connectPublicationsWithOrganizations(EntityManager entitymanager, Map<Long, Publication> pubs, Map<Long, Organization> orgs, List<Object[]> pubsToOrgs) {
		
		// connect publications with organizations
		entitymanager.getTransaction().begin();
		
		Map<Long, List<Publication>> mapOrgToPubs = new HashMap<>();  
		Map<String,String> coupleIdsAlreadyVisited = new HashMap<>();
		
		// loop over publication-organization connections
		for (Object[] pto: pubsToOrgs) {
			
			Long pubId = (Long) pto[0];
			Long orgId = (Long) pto[1];
			
			// patiris dataset is not cleaned: it may contains duplicated connections
			String key = pubId + "-" + orgId;
			// inserting new connections
			if (coupleIdsAlreadyVisited.get(key) == null) {
			
				if(pubId != null && orgId != null) {
					if (mapOrgToPubs.get(orgId) != null) {
						Publication p = pubs.get(pubId);
						if (p != null) {
							mapOrgToPubs.get(orgId).add(p);
						}
					} else {
						Publication p = pubs.get(pubId);
						if (p != null) {
							List<Publication> ps = new ArrayList<>();
							ps.add(p);
							mapOrgToPubs.put(orgId, ps);
						}
					}
				}	
				
				coupleIdsAlreadyVisited.put(key, "true");
			}
		}
		
		for (Long orgId: mapOrgToPubs.keySet()) {
			Organization org = orgs.get(orgId);
			if(org != null) {
				List<Publication> ps = mapOrgToPubs.get(orgId);
				if(ps != null) {
					org.setPublications(ps);
					entitymanager.merge(org);
				}
			}
		}
		
		entitymanager.getTransaction().commit();
	}
	
	private void connectPublicationsWithThematics(EntityManager entitymanager, Map<Long, Publication> pubs, Map<Long, Thematic> thematics, List<Object[]> pubsToThemes) {
		
		// connect publications with thematics 
		entitymanager.getTransaction().begin();
		
		Map<Long, List<Thematic>> mapPubToThematics = new HashMap<>();  
		for (Object[] ptt: pubsToThemes) {
			
			Long pubId = (Long) ptt[0];
			Long themeId = (Long) ptt[1];
			
			if(pubId != null && themeId != null) {
				if (mapPubToThematics.get(pubId) != null) {
					Thematic t = thematics.get(themeId);
					if (t != null) {
						mapPubToThematics.get(pubId).add(t);
					}
				} else {
					Thematic t = thematics.get(themeId);
					if (t != null) {
						List<Thematic> ts = new ArrayList<>();
						ts.add(t);
						mapPubToThematics.put(pubId, ts);
					}
				}
			}									
		}
		
		for (Long pubId: mapPubToThematics.keySet()) {
			
			Publication pub = pubs.get(pubId);
			if(pub != null) {
				List<Thematic> ts = mapPubToThematics.get(pubId);
				if(ts != null) {
					pub.setThematics(ts);
					entitymanager.merge(pub);
				}
			}
		}
		
		entitymanager.getTransaction().commit();
	}
		
	private void connectPublicationsWithPeople(EntityManager entitymanager, Map<Long, Publication> pubs, Map<Long, Person> invs, List<Object[]> pubsToPeople) {
		
		// connect authors with publications
		entitymanager.getTransaction().begin();
		
		Map<Long, List<Person>> mapPubToPeople = new HashMap<>();  
		for (Object[] ptp: pubsToPeople) {
			
			Long pubId = (Long) ptp[0];
			Long personId = (Long) ptp[1];
			
			if(pubId != null && personId != null) {
				if (mapPubToPeople.get(pubId) != null) {
					Person p = invs.get(personId);
					if (p != null) {
						mapPubToPeople.get(pubId).add(p);
					}
				} else {
					Person p = invs.get(personId);
					if (p != null) {
						List<Person> ps = new ArrayList<>();
						ps.add(p);
						mapPubToPeople.put(pubId, ps);
					}
				}
			}									
		}
		
		for (Long pubId: mapPubToPeople.keySet()) {
			Publication pub = pubs.get(pubId);
			if(pub != null) {
				List<Person> ps = mapPubToPeople.get(pubId);
				if(ps != null) {
					pub.setAuthors(ps);
					entitymanager.merge(pub);
				}
			}
		}
		
		entitymanager.getTransaction().commit();
	}

	private Source setSource() {
		
		// save data source information
		Source source = new Source();
		source.setLabel(sourceName);
		source.setUrl(this.sourceUrl);
		DateFormat df = new SimpleDateFormat("dd-MM-yyyy");

		Date sourceDate;
		try {
			sourceDate = df.parse(sourceRevisionDate);
			source.setRevisionDate(sourceDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		return source;
	}

	public void importData(String db) {

		// BEGIN retrieve patiris data ------------------------------------------------------------
		// initialize entity manager factory
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory("newpatiris");
		EntityManager entitymanager = emfactory.createEntityManager();
		entitymanager.getTransaction().begin();
		
		// retrieve patiris publications
		System.out.println("Starting retrieving publications...");
		Query qPat = entitymanager.createNativeQuery("SELECT * FROM patent");
		List<Object[]> patents = qPat.getResultList();
		System.out.println("Retrieved " + patents.size() + " publications.");
		
		// retrieve patiris organizations
		System.out.println("Starting retrieving organizations...");
		Query qOrg = entitymanager.createNativeQuery("SELECT * FROM anagrafica_miur");
		List<Object[]> retrievedOrgs = qOrg.getResultList();
		System.out.println("Retrieved " + retrievedOrgs.size() + " organizations.");
		
		// retrieve patiris thematics
		System.out.println("Starting retrieving thematics...");
		Query qIpc = entitymanager.createNativeQuery("SELECT * FROM ipc");
		List<Object[]> ipcThematics = qIpc.getResultList();
		System.out.println("Retrieved " + ipcThematics.size() + " thematics.");
		
		// retrieve patiris people
		System.out.println("Starting retrieving people...");
		Query qInv = entitymanager.createNativeQuery("SELECT * FROM inventor");
		List<Object[]> retrievedPeople = qInv.getResultList();
		System.out.println("Retrieved " + retrievedPeople.size() + " people.");
		
		// retrieve connections between patiris publications and organizations
		System.out.println("Starting retrieving publications and organizations connections...");
		Query qPubsToOrgs = entitymanager.createNativeQuery("SELECT * FROM pat_epr");
		List<Object[]> pubsToOrgs = qPubsToOrgs.getResultList();
		System.out.println("Retrieved " + pubsToOrgs.size() + " connections.");
		
		// retrieve connections between patiris publications and thematics
		System.out.println("Starting retrieving publications and thematics connections...");
		Query qPubsToThemes = entitymanager.createNativeQuery("SELECT * FROM pat_ipc");
		List<Object[]> pubsToThemes = qPubsToThemes.getResultList();
		System.out.println("Retrieved " + pubsToThemes.size() + " connections.");
		
		// retrieve connections between patiris publications and people
		System.out.println("Starting retrieving publications and people connections...");
		Query qPubsToPeople = entitymanager.createNativeQuery("SELECT * FROM pat_inv");
		List<Object[]> pubsToPeople = qPubsToPeople.getResultList();
		System.out.println("Retrieved " + pubsToPeople.size() + " connections.");
		
		entitymanager.getTransaction().commit();	
		entitymanager.close();
		emfactory.close();
		// END retrieve patiris data --------------------------------------------------------------
		
		// BEGIN import data in new db ------------------------------------------------------------	
		// initialize entity manager factory
		emfactory = Persistence.createEntityManagerFactory(db);
		entitymanager = emfactory.createEntityManager();
		
		entitymanager.getTransaction().begin();
		
		Query query = entitymanager.createQuery("Select s FROM Source s where s.label = :label");
		query.setParameter("label", sourceName);
		List<Source> patirisSource = query.getResultList();
		Source source = null;
		if (patirisSource.size() > 0) {
			source = patirisSource.get(0);
			System.out.println("Retrieved " + source.getLabel() + " source");
		} else {
			source = setSource();
			entitymanager.persist(source);
			System.out.println("Created " + source.getLabel() + " source");
		}
		List<Source> sources = new ArrayList<Source>();
		sources.add(source);
		
		entitymanager.getTransaction().commit();
		
		Map<Long, Publication> pubs = null;
		Map<Long, Organization> orgs = null;
		Map<Long, Thematic> thematics = null;
		Map<Long, Person> invs = null;
		
		if (patents != null) {
			System.out.println("Starting importing publications...");
			pubs = importPublications(entitymanager, sources, patents);
			System.out.println("Imported " + pubs.size() + " publications.");
		}
		if (retrievedOrgs != null) {
			System.out.println("Starting importing organizations...");
			orgs = importOrganizations(entitymanager, sources, retrievedOrgs);
			System.out.println("Imported " + orgs.size() + " organizations.");
		}
		if (ipcThematics != null) {
			System.out.println("Starting importing thematics...");
			thematics = importPublicationThematics(entitymanager, ipcThematics);
			System.out.println("Imported " + thematics.size() + " thematics.");
		}
		if (retrievedPeople != null) {
			System.out.println("Starting importing people...");
			invs = importPeople(entitymanager, sources, retrievedPeople);
			System.out.println("Imported " + invs.size() + " people.");
		}
		
		// connect entities
		if (pubs != null && orgs != null && pubsToOrgs != null) {
			System.out.println("Starting importing publications and organizations connections...");
			connectPublicationsWithOrganizations(entitymanager, pubs, orgs, pubsToOrgs);
		}
		if (pubs != null && thematics != null && pubsToThemes != null) {
			System.out.println("Starting importing publications and thematics connections...");
			connectPublicationsWithThematics(entitymanager, pubs, thematics, pubsToThemes);
		}
		if (pubs != null && invs != null && pubsToPeople != null) {
			System.out.println("Starting importing publications and people connections...");
			connectPublicationsWithPeople(entitymanager, pubs, invs, pubsToPeople);
		}
			
		System.out.println("Patiris data import completed.");
		entitymanager.close();
		emfactory.close();	
		// END import data in new db --------------------------------------------------------------

	}

}
