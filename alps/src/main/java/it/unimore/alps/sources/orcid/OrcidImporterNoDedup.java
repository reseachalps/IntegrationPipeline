package it.unimore.alps.sources.orcid;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import it.unimore.alps.sources.cnr.CnrImporter;
import it.unimore.alps.sql.model.Badge;
import it.unimore.alps.sql.model.Leader;
import it.unimore.alps.sql.model.Link;
import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationActivity;
import it.unimore.alps.sql.model.OrganizationExtraField;
import it.unimore.alps.sql.model.OrganizationIdentifier;
import it.unimore.alps.sql.model.OrganizationRelation;
import it.unimore.alps.sql.model.OrganizationType;
import it.unimore.alps.sql.model.Person;
import it.unimore.alps.sql.model.PersonIdentifier;
import it.unimore.alps.sql.model.Project;
import it.unimore.alps.sql.model.ProjectExtraField;
import it.unimore.alps.sql.model.ProjectIdentifier;
import it.unimore.alps.sql.model.Publication;
import it.unimore.alps.sql.model.PublicationIdentifier;
import it.unimore.alps.sql.model.Source;
import it.unimore.alps.sql.model.SpinoffFrom;
import it.unimore.alps.sql.model.Thematic;
import it.unimore.alps.sql.model.Theme;

public class OrcidImporterNoDedup {

	private Reader reader;									// orcid dump reader
	private String sourceName = "ORCID";					// data source name 
	private String sourceUrl = "https://orcid.org/";		// data source url
	private String sourceRevisionDate = "01-07-2018";		// data source date

	public static void main(String[] args) {

		// BEGIN: INPUT PARAMETERS ------------------------------------------------------
		CommandLine commandLine;
		// orcid dump file
		Option fileOption = Option.builder("file").hasArg().required(true)
				.desc("The file that contains a dump of ORCID dataset. ").longOpt("file").build();
		// database where to import data
		Option DB = Option.builder("DB").hasArg().required(true).desc("DB. ").longOpt("DB").build();

		Options options = new Options();
		CommandLineParser parser = new DefaultParser();

		options.addOption(fileOption);
		options.addOption(DB);

		String file = null;
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

			if (commandLine.hasOption("file")) {
				file = commandLine.getOptionValue("file");
				System.out.println("\tOrcid dump file: " + file);
			} else {
				System.out.println("\tOrcid dump file not provided. Use the file option.");
				System.exit(1);
			}		

			System.out.println("----------------------------\n");

		} catch (org.apache.commons.cli.ParseException e) {
			e.printStackTrace();
		}
		// END: INPUT PARAMETERS --------------------------------------------------------

		// import data
		System.out.println("Starting importing ORCID data...");
		OrcidImporterNoDedup orcidImporter = new OrcidImporterNoDedup(file);
		orcidImporter.importData(db);

	}

	public OrcidImporterNoDedup(String file) {

		try {
			this.reader = new FileReader(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

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
		} catch (java.text.ParseException e) {
			e.printStackTrace();
		}

		return source;
	}
	
	private Person setPersonFields(JSONObject data) {
		
		// save person information into Person object
		Person person = new Person();
		
		JSONObject orcid_profile = (JSONObject) data.get("orcid-profile");
		if (orcid_profile != null) {
			JSONObject orcid_bio = (JSONObject) orcid_profile.get("orcid-bio");
			if (orcid_bio != null) {
				JSONObject personal_details = (JSONObject) orcid_bio.get("personal-details");
				if (personal_details != null) {
					
					JSONObject given_names = (JSONObject) personal_details.get("given-names");
					if (given_names != null) {
						String firstName = (String) given_names.get("value");
						if (firstName != null) {
							person.setFirstName(firstName);
						}
					}
					
					JSONObject family_name = (JSONObject) personal_details.get("family-name");
					if (family_name != null) {
						String lastName = (String) family_name.get("value");
						if (lastName != null) {
							person.setLastName(lastName);
						}
					}
				}
				
				JSONObject contact_details = (JSONObject) orcid_bio.get("contact-details");
				if (contact_details != null) {
					JSONArray email = (JSONArray) contact_details.get("email");
					if (email != null) {
						Iterator<JSONObject> it = email.iterator();
						while (it.hasNext()) {														
							JSONObject personEmail = it.next();
							if (personEmail != null) {
								String pEmail = (String) personEmail.get("value");
								if (pEmail != null) {
									person.setEmail(pEmail);
									break;
								}
							}
						}

						
					}
				}								
			}
		}
		
		return person;
	}
	
	private List<PersonIdentifier> setPersonIdentifiers(JSONObject data, Person person) {

		// save person identifier into PersonIdentifier object
		List<PersonIdentifier> personIds = new ArrayList<PersonIdentifier>();

		JSONObject orcid_profile = (JSONObject) data.get("orcid-profile");
		if (orcid_profile != null) {
			
			JSONObject orcid_identifier = (JSONObject) orcid_profile.get("orcid-identifier");
			if (orcid_identifier != null) {
				String path = (String) orcid_identifier.get("uri");
				if (path != null) {
					if (path.length()<255) {
						PersonIdentifier personId = new PersonIdentifier();
						personId.setIdentifier(path);
						personId.setIdentifierName("Orcid Id");
						personId.setPerson(person);
						personId.setProvenance(sourceName);
						personIds.add(personId);
					}
				}	
			}
			
			JSONObject orcid_bio = (JSONObject) orcid_profile.get("orcid-bio");
			if (orcid_bio != null) {
				JSONObject researcher_urls = (JSONObject) orcid_bio.get("researcher-urls");
				if (researcher_urls != null) {
					JSONArray researcher_url = (JSONArray) researcher_urls.get("researcher-url");
					if (researcher_url != null) {
						Iterator<JSONObject> it = researcher_url.iterator();
						
						// read JSON object line by line
						while (it.hasNext()) {
							
							JSONObject rUrl = it.next();
							if (rUrl != null) {
								
								PersonIdentifier personId = null;
								JSONObject url_value = (JSONObject) rUrl.get("url");
								if (url_value != null) {
									String url = (String) url_value.get("value");
									if (url != null) {
										if (url.length() < 255) {
											personId = new PersonIdentifier();
											personId.setIdentifier(url);
										}
									}
								}
								
								if (personId != null) {
								
									JSONObject url_name = (JSONObject) rUrl.get("url-name");
									if (url_name != null) {
										String source_url = (String) url_name.get("value");
										if (source_url != null) {	
											if (source_url.length() < 255) {
												personId.setIdentifierName(source_url);
											}
										}
									}
																									
									personId.setProvenance(sourceName);
									personId.setPerson(person);
									personIds.add(personId);
								}
							}
						}
							 
					}
				}
				
				JSONObject external_identifiers = (JSONObject) orcid_bio.get("external-identifiers");				
				if (external_identifiers != null) {
					JSONArray external_identifier = (JSONArray) external_identifiers.get("external-identifier");
					if (external_identifier != null) {
						Iterator<JSONObject> it = external_identifier.iterator();
						
						// read JSON object line by line
						while (it.hasNext()) {							
							
							JSONObject ei = it.next();
							if (ei != null) {
								
								PersonIdentifier personId = null;
								
								JSONObject external_id_reference = (JSONObject) ei.get("external-id-reference");
								if (external_id_reference != null) {
									String external_id = (String) external_id_reference.get("value");
									if (external_id != null) {
										if (external_id.length() < 255) {
											personId = new PersonIdentifier();
											personId.setIdentifier(external_id);
										}
									}
								}								
								
								if (personId != null) {
									JSONObject external_id_common_name = (JSONObject) ei.get("external-id-common-name");
									if (external_id_common_name != null) {
										String external_id_source = (String) external_id_common_name.get("value");
										if (external_id_source != null) {
											if (external_id_source.length() < 255) {
												personId.setIdentifierName(external_id_source);	
											}
										}
									}
									
									personId.setProvenance(sourceName);
									personId.setPerson(person);
									personIds.add(personId);
								}																
							}																					
						}
					}
				}
			}									
		}

		return personIds;

	}
	
	private List<PublicationIdentifier> setPublicationIdentifiers(JSONObject data, Publication pub) {
		
		// save publication identifier information into PublicationIdentifier objects
		List<PublicationIdentifier> pubIds = new ArrayList<>();
		
		JSONObject work_external_identifiers = (JSONObject) data.get("work-external-identifiers");
		if (work_external_identifiers != null) {
			
			JSONArray work_external_identifier = (JSONArray) work_external_identifiers.get("work-external-identifier");
			if (work_external_identifier != null) {
				Iterator<JSONObject> itIds = work_external_identifier.iterator();
				
				// read JSON object line by line
				while (itIds.hasNext()) {
					JSONObject weid = itIds.next();
					if (weid != null) {
						PublicationIdentifier pubId = null;
						JSONObject identifier = (JSONObject) weid.get("work-external-identifier-id");
						if (identifier != null) {
							String pubIdentifier = (String) identifier.get("value");
							if (pubIdentifier != null) {
								if (pubIdentifier.length() < 255) {
									pubId = new PublicationIdentifier();
									pubId.setIdentifier(pubIdentifier);
								}
							}
						}
						
						if (pubId != null) {
						
							String provenance = (String) weid.get("work-external-identifier-type");
							if (provenance != null) {
								pubId.setIdentifierName(provenance);
							}						
													
							pubId.setProvenance(sourceName);
							pubId.setPublication(pub);
							pubIds.add(pubId);
						}
					}
				}
			}
		}
		
		return pubIds;
	}
	
	private String getDoi(JSONObject data) {
		
		// extract publication DOI
		String doi = null;		
		JSONObject work_external_identifiers = (JSONObject) data.get("work-external-identifiers");
		if (work_external_identifiers != null) {
			
			JSONArray work_external_identifier = (JSONArray) work_external_identifiers.get("work-external-identifier");
			if (work_external_identifier != null) {
				Iterator<JSONObject> itIds = work_external_identifier.iterator();
				while (itIds.hasNext()) {
					JSONObject weid = itIds.next();
					if (weid != null) {												
						
						String provenance = (String) weid.get("work-external-identifier-type");
						JSONObject identifier = (JSONObject) weid.get("work-external-identifier-id");
						if (provenance != null && identifier != null) {
							String pubIdentifier = (String) identifier.get("value");
							if (pubIdentifier != null && provenance.equals("doi")) {
								doi = pubIdentifier.toLowerCase();
								break;
							}
						}						
					}
				}
			}
		}
		
		return doi;
	}
	
	private Publication setPublication(JSONObject ow, Map<String,Publication> mapPubs, List<Source> sources, Person person, EntityManager entitymanager) {
		
		// save publication information into Publication object
		Publication pub = new Publication();
		
		JSONObject work_title = (JSONObject) ow.get("work-title");
		if (work_title != null) {
			// title
			JSONObject title = (JSONObject) work_title.get("title");
			if (title != null) {
				String pubTitle = (String) title.get("value");
				if (pubTitle != null) {
					pub.setTitle(pubTitle);
				}
			}
			
			// subtitle
			JSONObject subtitle = (JSONObject) work_title.get("subtitle");
			if (subtitle != null) {
				String pubSubtitle = (String) subtitle.get("value");
				if (pubSubtitle != null) {
					pub.setSubtitle(pubSubtitle);
				}
			}
		}
		
		// location name
		JSONObject journal_title = (JSONObject) ow.get("journal-title");
		if (journal_title != null) {
			String journal_name = (String) journal_title.get("value");
			if (journal_name != null) {
				pub.setLocationName(journal_name);
			}																	
		}
		
		// location type
		String work_type = (String) ow.get("work-type");
		if (work_type != null) {
			pub.setLocationType(work_type);
		}
		
		// description
		String descr = (String) ow.get("short-description");
		if (descr != null) {
			pub.setDescription(descr);
		}
		
		// publication date
		String dateYear = null;
		String dateMonth = null;
		String dateDay = null;
		JSONObject publication_date = (JSONObject) ow.get("publication-date");
		if (publication_date != null) {
			JSONObject year = (JSONObject) publication_date.get("year");									
			if (year != null) {
				String pubYear = (String) year.get("value");
				if (pubYear != null) {
					dateYear = pubYear;
				}
			}
			
			JSONObject month = (JSONObject) publication_date.get("month");									
			if (month != null) {
				String pubMonth = (String) month.get("value");
				if (pubMonth != null) {
					dateMonth = pubMonth;
				}
			}
			
			JSONObject day = (JSONObject) publication_date.get("day");									
			if (day != null) {
				String pubDay = (String) day.get("value");
				if (pubDay != null) {
					dateDay = pubDay;
				}
			}
		}
		
		if(dateYear != null) {
			if (dateMonth == null) {
				dateMonth = "01";
			}
			if (dateDay == null) {
				dateDay = "01";
			}
			
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			String dateInString = dateYear + "-" + dateMonth + "-" + dateDay;

	        try {

	            Date date = formatter.parse(dateInString);
	            pub.setPublicationDate(date);

	        } catch (java.text.ParseException e) {
	            e.printStackTrace();
	        }
		}
		
		// url
		JSONObject url = (JSONObject) ow.get("url");
		if (url != null) {
			String pubUrl = (String) url.get("value");
			if (pubUrl != null) {
				pub.setUrl(pubUrl);
			}																	
		}
		
		// publication type
		pub.setType("publication");	
		
		// sources
		if (pub != null) {
			pub.setSources(sources);
		}
		
		// authors
		List<Person> authors = new ArrayList<>();
		authors.add(person);
		pub.setAuthors(authors);
		
		// publication identifiers
		List<PublicationIdentifier> pubIds = setPublicationIdentifiers(ow, pub);					
		// create PersonIdentifier tuples in the DB
		for (PublicationIdentifier pubId: pubIds) {
			// adding publication among visited publications
			if(pubId.getProvenance().equals("doi")) {
				mapPubs.put(pubId.getIdentifier(), pub);
			}
			entitymanager.persist(pubId);
		}
						
		return pub;
	}
	
	private Date getAffiliationStartDate(JSONObject data) {
		
		// retrieve author affiliation start date
		Date date = null;
		String dateYear = null;
		String dateMonth = null;
		String dateDay = null;
		JSONObject start_date = (JSONObject) data.get("start-date");
		if (start_date != null) {

			JSONObject year = (JSONObject) start_date.get("year");									
			if (year != null) {
				String affYear = (String) year.get("value");
				if (affYear != null) {
					dateYear = affYear;
				}
			}
			
			JSONObject month = (JSONObject) start_date.get("month");									
			if (month != null) {
				String affMonth = (String) month.get("value");
				if (affMonth != null) {
					dateMonth = affMonth;
				}
			}
			
			JSONObject day = (JSONObject) start_date.get("day");									
			if (day != null) {
				String affDay = (String) day.get("value");
				if (affDay != null) {
					dateDay = affDay;
				}
			}
		}
		
		if(dateYear != null) {
			if (dateMonth == null) {
				dateMonth = "01";
			}
			if (dateDay == null) {
				dateDay = "01";
			}
			
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			String dateInString = dateYear + "-" + dateMonth + "-" + dateDay;

	        try {
	            date = formatter.parse(dateInString);							            
	        } catch (java.text.ParseException e) {
	            e.printStackTrace();
	        }
		}
		
		return date;
	}
	
	private void setOrgAddress(JSONObject data, Organization organization, List<Source> sources) {
		
		// save organization address information
		JSONObject address = (JSONObject) data.get("address");
			
		if (address != null) {
			String city = (String) address.get("city");
			if (city != null) {
				String cleanCity = city.substring(0, 1).toUpperCase() + city.substring(1);
				organization.setCity(cleanCity);	
			}
			/*String region = (String) address.get("region");
			if (region != null) {
				organization.setUrbanUnit(region);
			}*/
			String countryCode = (String) address.get("country");
			if (countryCode != null) {
				organization.setCountryCode(countryCode);
				
				if (countryCode.equals("AT")) {
					organization.setCountry("Austria");
		    	}
		    	if (countryCode.equals("CH")) {
		    		organization.setCountry("Switzerland");
		    	}
		    	if (countryCode.equals("DE")) {
		    		organization.setCountry("Germany");
		    	}
		    	if (countryCode.equals("FR")) {
		    		organization.setCountry("France");
		    	}
		    	if (countryCode.equals("IT")) {
		    		organization.setCountry("Italy");
		    	}
		    	if (countryCode.equals("LI")) {
		    		organization.setCountry("Liechtenstein");
		    	}
		    	if (countryCode.equals("SI")) {
		    		organization.setCountry("Slovenia");
		    	}
				
			}
			organization.setAddressSources(sources);
		}

		
	}
	
	private String getParentOrganizationName(JSONObject data) {
		
		// extract parent organization name
		String name = null;
		
		JSONObject org = (JSONObject) data.get("organization");
		if (org != null) {
			
			String orgName = (String) org.get("name");
			if (orgName != null) {
				if (orgName.length() < 255) {
					name = orgName;
				}
			}
		}
		
		return name;
	}
	
	private Organization importParentOrganization(JSONObject data, List<Source> sources) {
		
		// save organization parent information into Organization object
		Organization organizationParent = new Organization();
		
		String orgName = getParentOrganizationName(data); 
		if (orgName != null) {
			organizationParent.setLabel(orgName);
		}
		
		JSONObject org = (JSONObject) data.get("organization");
		if (org != null) {			
			setOrgAddress(org, organizationParent, sources);
		}				
		
		return organizationParent;
	}
	
	private Organization importChildOrganization(JSONObject data, List<Source> sources) {
		
		// save department (child organization) information into Organization object
		Organization organizationChild = new Organization();
		
		String department = (String) data.get("department-name");
		if (department != null) {
			if (department.length() < 255) {
				organizationChild.setLabel(department);
			}
		} else {
			return null;
		}
		
		JSONObject org = (JSONObject) data.get("organization");
		if (org != null) {
			setOrgAddress(org, organizationChild, sources);
		}
						
		return organizationChild;
		
	}
	
	private Organization getOrganizationInChildrenOrganizations(Organization pOrg, Organization cOrg) {
		
		// retrieve organization from child organizations
		Organization findOrganization = null;		
		List<Organization> childrenOrgs = pOrg.getChildrenOrganizations();
		
		if (childrenOrgs != null) {
			for (Organization org: childrenOrgs) {
				if (cOrg.getLabel().equals(org.getLabel())) {
					findOrganization = org;
					break;
				}
			}
		}
		
		return findOrganization;
	}
	
	private void connectOrganizations(Organization parent, Organization child) {
		
		// create parent-child relationship
		// connect organizations only if the connection doesn't already exist 
		List<Organization> childrenOrgs = parent.getChildrenOrganizations();
		if (childrenOrgs != null) {
			if (getOrganizationInChildrenOrganizations(parent, child) == null) {
				List<Organization> newChildrenOrgs = parent.getChildrenOrganizations();
				newChildrenOrgs.add(child);
				parent.setChildrenOrganizations(newChildrenOrgs);				
			}
		} else {
			List<Organization> newChildrenOrgs = new ArrayList<>();
			newChildrenOrgs.add(child);
			parent.setChildrenOrganizations(newChildrenOrgs);
		}
		
	}
	
	private String getRinggoldId(JSONObject data) {
		
		String ringgoldId = null;
		
		JSONObject org = (JSONObject) data.get("organization");
		if (org != null) {	
			JSONObject disambiguated_organization = (JSONObject) org.get("disambiguated-organization");
			if (disambiguated_organization != null) {
				String uniqueOrgId = (String) disambiguated_organization.get("disambiguated-organization-identifier");
				String uniqueOrgSource = (String) disambiguated_organization.get("disambiguation-source");
				if (uniqueOrgId != null && uniqueOrgSource != null) {
					if (uniqueOrgSource.equals("RINGGOLD")) {						
						ringgoldId = uniqueOrgId;
					}
				}
			}			
		}	
		
		return ringgoldId;
	}
	
	private String getAuthorOrcidId(Person author) {
		
		// retrieve author ORCID id
		String orcidId = null;
		for (PersonIdentifier pi : author.getPersonIdentifiers()) {
			if (pi.getIdentifierName() != null) {
				if (pi.getIdentifierName().equals("Orcid Id")) {
					orcidId = pi.getIdentifier();
				}
			}
		}
		
		return orcidId;
	}
	
	private Person getPersonInPublicationAuthorList(Person person, Publication pub) {
		
		// retrieve publication co-author
		Person findPerson = null;
		
		List<Person> authors = pub.getAuthors();
		if (authors != null) {
			for(Person author: authors) {
				String authorOrcidId = getAuthorOrcidId(author);
				String personOrcidId = getAuthorOrcidId(person); 
				if (authorOrcidId != null && personOrcidId != null) {
					if (authorOrcidId.equals(personOrcidId)) {
						findPerson = author;
						break;
					}
				}
			}
		}
		
		return findPerson;
	}
	
	private List<Publication> importPublications(JSONObject data, EntityManager entitymanager, List<Source> sources, Person person, Map<String,Publication> mapPubs) {
				
		List<Publication> pubs = new ArrayList<>();	
		
		JSONObject orcid_works = (JSONObject) data.get("orcid-works");
		if (orcid_works != null) {
			
			JSONArray orcid_work = (JSONArray) orcid_works.get("orcid-work");
			if (orcid_work != null) {
				Iterator<JSONObject> it = orcid_work.iterator();
				
				// read JSON object line by line
				while (it.hasNext()) {
					
					// JSON data row
					JSONObject ow = it.next();
					if (ow != null) {
														
						String doi = getDoi(ow);
						Publication pub = mapPubs.get(doi);
						if (doi != null && pub != null) {					// publication already visited
							// update the list of authors
							if (getPersonInPublicationAuthorList(person, pub) == null) {
								List<Person> authors = pub.getAuthors();
								authors.add(person);
								pub.setAuthors(authors);
								mapPubs.put(doi, pub);
							}
						} else {											// new publication								
							pub = setPublication(ow, mapPubs, sources, person, entitymanager);	
							if (doi == null) {
								pubs.add(pub);
							} else {
								mapPubs.put(doi, pub);
							}
						}
					}							
				}
			}
		}
		
		return pubs;
		
	}
	
	private JSONObject getMostRecentAffiliation(JSONObject data) {
		
		// retrieve most recent person-organization affiliation
		boolean firstIteration = true;
		Date recentAffiliationDate = null;
		JSONObject recentAffiliation = null;
		JSONObject affiliations = (JSONObject) data.get("affiliations");
		if (affiliations != null) {
			
			JSONArray affiliation = (JSONArray) affiliations.get("affiliation");
			if (affiliation != null) {
				Iterator<JSONObject> itAff = affiliation.iterator();						
				
				// read JSON object line by line
				while (itAff.hasNext()) {
					
					// JSON data row
					JSONObject aff = itAff.next();
					if (aff != null) {
														
						Date date = getAffiliationStartDate(aff);
						
						if (firstIteration) {
							recentAffiliationDate = date;
							recentAffiliation = aff;
							firstIteration = false;
							if (recentAffiliationDate == null) {
								break;
							}
						} else {						
							if (date != null) {
								if (date.after(recentAffiliationDate)) {
									recentAffiliationDate = date;
									recentAffiliation = aff;
								}
							}
						}
					}																							
				}
			}						
		}
		
		return recentAffiliation;
		
	}
	
	private List<OrganizationIdentifier> setOrganizationIdentifiers(JSONObject data, Organization organization) {
		
		// save organization identifier information into OrganizationIdentifier object
		List<OrganizationIdentifier> orgIds = new ArrayList<>();
		
		JSONObject org = (JSONObject) data.get("organization");
		if (org != null) {				
			
			JSONObject disambiguated_organization = (JSONObject) org.get("disambiguated-organization");
			if (disambiguated_organization != null) {
				
				OrganizationIdentifier orgId = new OrganizationIdentifier();
				
				String uniqueOrgId = (String) disambiguated_organization.get("disambiguated-organization-identifier");
				if (uniqueOrgId != null) {
					orgId.setIdentifier(uniqueOrgId);
				}
				String uniqueOrgSource = (String) disambiguated_organization.get("disambiguation-source");
				if (uniqueOrgSource != null) {					
					orgId.setIdentifierName(uniqueOrgSource);
				}
				
				orgId.setOrganization(organization);
				orgId.setProvenance(sourceName);
				orgIds.add(orgId);
			}			
		}
		
		return orgIds;
		
	}
	
	private boolean isValidAffiliation(JSONObject data) {
		
		// check if the affiliation refer to Alpine region organizations
		boolean valid = false;
		List<String> myList = Arrays.asList("IT", "FR", "AT", "DE", "CH", "SI", "LI");
		
		JSONObject orcid_profile = (JSONObject) data.get("orcid-profile");
		if (orcid_profile != null) {
			JSONObject orcid_activities = (JSONObject) orcid_profile.get("orcid-activities");
			if (orcid_activities != null) {
				// get most recent affiliation
				JSONObject recentAffiliation = getMostRecentAffiliation(orcid_activities);
				if (recentAffiliation != null) {
					JSONObject org = (JSONObject) recentAffiliation.get("organization");
					if (org != null) {
						//System.out.println(org);
						JSONObject address = (JSONObject) org.get("address");
						
						if (address != null) {
							
							String country = (String) address.get("country");
							if (country != null) {
								for(String countryCode: myList) {
									if (country.equalsIgnoreCase(countryCode)) {
										valid = true;
										break;
									}
								}
							}
						}
					}
				}
			}
		}				
		
		return valid;
		
	}
	
	private List<Organization> importOrganizations(JSONObject data, EntityManager entitymanager, List<Source> sources, Person person, Map<String,Organization> mapOrgs) {
		
		List<Organization> orgs = new ArrayList<>();
		
		// get most recent affiliation
		JSONObject recentAffiliation = getMostRecentAffiliation(data);
		
		if (recentAffiliation != null) {
			Organization organizationParent = null;
			Organization organizationChild = null;
			
			String orgName = getParentOrganizationName(recentAffiliation); 
			if (orgName != null && mapOrgs.get(orgName) != null) {		// parent organization already visited				
				organizationParent = mapOrgs.get(orgName);
				organizationChild = importChildOrganization(recentAffiliation, sources);				
				
				if (organizationChild != null) {
					
					// although parent organization was already visited, child organization may be a new organization
					Organization foundOrg = null;
					if ((foundOrg = getOrganizationInChildrenOrganizations(organizationParent, organizationChild)) != null) {	// child organization already visited
						// update people
						organizationChild = foundOrg;
						if (organizationChild != null) {
							List<Person> people = organizationChild.getPeople();
							if (people != null) {
								people.add(person);
								organizationChild.setPeople(people);
							}
						}					
					} else {				// new child organization
						organizationChild = importChildOrganization(recentAffiliation, sources);
						
						// sources
						if (organizationChild != null) {
							organizationChild.setSources(sources);
						}
						
						// people
						if (organizationChild != null) {
							List<Person> people = new ArrayList<>();
							people.add(person);
							organizationChild.setPeople(people);
						}
					}
					connectOrganizations(organizationParent, organizationChild);
										
					mapOrgs.put(orgName + "_" + organizationChild.getLabel(), organizationChild);
					
				} else {
					// people
					if (organizationParent != null) {
						List<Person> people = null;
						if (organizationParent.getPeople() != null) {
							people = organizationParent.getPeople();
						} else {
							people = new ArrayList<>();
						}
						people.add(person);
						organizationParent.setPeople(people);
					}
				}
				
				mapOrgs.put(orgName, organizationParent);				
				
			} else {															// new parent organization
				// create parent and child organizations
				organizationParent = importParentOrganization(recentAffiliation, sources);
				
				// organization identifiers
				List<OrganizationIdentifier> orgIds = setOrganizationIdentifiers(recentAffiliation, organizationParent);
				// create OrganizationIdentifier tuples in the DB
				for (OrganizationIdentifier orgIdentifier : orgIds) {
					entitymanager.persist(orgIdentifier);
				}
				
				// sources
				if (organizationParent != null) {
					organizationParent.setSources(sources);
				}
				
				organizationChild = importChildOrganization(recentAffiliation, sources);															
				
				if (organizationChild != null) {
					// sources
					organizationChild.setSources(sources);
					
					// people
					List<Person> people = new ArrayList<>();
					people.add(person);
					organizationChild.setPeople(people);

					connectOrganizations(organizationParent, organizationChild);
				
				} else {
					// people
					if (organizationParent != null) {
						List<Person> people = new ArrayList<>();
						people.add(person);
						organizationParent.setPeople(people);
					}
				}			
				
				// if parent ringgold id exists, i use this identifier to disambiguate on-the-fly parent organizations
				if (orgName != null) { 
					mapOrgs.put(orgName, organizationParent);
					if (organizationChild != null) {
						mapOrgs.put(orgName + "_" + organizationChild.getLabel(), organizationChild);
					}
				} else {	// organizations without ringgold id or children organizations
					
					if (organizationChild != null) {
						orgs.add(organizationChild);
					}
					orgs.add(organizationParent);
				}												
			}
		}		
		
		return orgs;
		
	}
	
	private void importPublicationsAndOrganizations(JSONObject data, EntityManager entitymanager, List<Source> sources, Person person, Map<String, Organization> mapOrgs, Map<String,Publication> mapPubs) {	
		
		JSONObject orcid_profile = (JSONObject) data.get("orcid-profile");
		if (orcid_profile != null) {
			JSONObject orcid_activities = (JSONObject) orcid_profile.get("orcid-activities");
			if (orcid_activities != null) {
				
				List<Publication> pubs = importPublications(orcid_activities, entitymanager, sources, person, mapPubs);
				for (Publication pub: pubs) {
					entitymanager.persist(pub);
				}
				
				List<Organization> orgs = importOrganizations(orcid_activities, entitymanager, sources, person, mapOrgs);
				for (Organization org: orgs) {
					entitymanager.persist(org);
				}				
			}
		}
		
	}
	
	private void importOrcidEntities(EntityManager entitymanager, List<Source> sources) {
		
		int count = 0;
		
		Map<String, Organization> mapOrgs = new HashMap<>();
		Map<String,Publication> mapPubs = new HashMap<>();

		System.out.println("Starting importing ORCID people, publications and organizations...");
		
		entitymanager.getTransaction().begin();
		
		// read orcid file line by line
		BufferedReader br = null;
		try {

			br = new BufferedReader(reader);

			String sCurrentLine;
			
			// JSON row data
			while ((sCurrentLine = br.readLine()) != null) {
				
				if ((count % 1000) == 0) {
					System.out.println(count);
				}
				count++;
				
				JSONParser parser = new JSONParser();
				JSONObject personData;
				try {
					personData = (JSONObject) parser.parse(sCurrentLine);
					
					if(isValidAffiliation(personData)) {
					
						// set main organization fields
						Person person = setPersonFields(personData);
						
						if (person != null) {				
							
							// set person identifiers
							List<PersonIdentifier> personIdentifiers = setPersonIdentifiers(personData, person);
							// create PersonIdentifier tuples in the DB
							for (PersonIdentifier personIdentifier: personIdentifiers) {
								entitymanager.persist(personIdentifier);
							}
							person.setPersonIdentifiers(personIdentifiers);
						
							// connect the source to the related person
							person.setSources(sources);					
						
							// import publications and organizations related to current person
							importPublicationsAndOrganizations(personData, entitymanager, sources, person, mapOrgs, mapPubs);														
	
							// persist person
							entitymanager.persist(person);
						}
					}
					
				} catch (ParseException e) {
					System.out.println("Error in parsing:\n" + sCurrentLine);
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}		
		
		// persist organizations
		for(String key: mapOrgs.keySet()) {
			Organization org = mapOrgs.get(key);
			if (org != null) {
				entitymanager.persist(org);
			}
		}
		
		// persist publications
		for(String key: mapPubs.keySet()) {
			Publication pub = mapPubs.get(key);
			if (pub != null) {
				entitymanager.persist(pub);
			}
		}
		
		entitymanager.getTransaction().commit();
		
		System.out.println("ORCID data imported.");
		
	}
	
	public static Source getSourceByName(EntityManager entitymanager, String sourceName) {
		
		// retrieve data source from database by name
		Query query = entitymanager.createQuery("Select s FROM Source s where s.label=:sourceLabel");
		query.setParameter("sourceLabel", sourceName);
		List<Source> sources = query.getResultList();
		Source source = sources.get(0);
		return source;
	}
	
	public static List<Organization> retrieveOrganizations(EntityManager entitymanager, Source source) {

		// retrieve from database organizations appertaining to input data source
		entitymanager.getTransaction().begin();
		Query query = entitymanager.createQuery("Select o FROM Organization o WHERE :id MEMBER OF o.sources");
		query.setParameter("id", source);
		List<Organization> orgs = query.getResultList();

		entitymanager.getTransaction().commit();
		System.out.println("Retrieved " + orgs.size() + " organizations");

		return orgs;
	}
	
	public static String getPeopleIdentifiers(Organization org) {
		
		// retrieve people identifiers
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

	public void importData(String db) {

		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(db);
		EntityManager entitymanager = emfactory.createEntityManager();

		// get or create ORCID source
		// ---------------------------------------------------------------
		entitymanager.getTransaction().begin();
		Query query = entitymanager.createQuery("Select s FROM Source s where s.label = :label");
		query.setParameter("label", sourceName);
		List<Source> orcidSource = query.getResultList();
		Source source = null;
		if (orcidSource.size() > 0) {
			source = orcidSource.get(0);
			System.out.println("Retrieved " + source.getLabel() + " source");
		} else {
			source = setSource();
			entitymanager.persist(source);
			System.out.println("Created " + source.getLabel() + " source");
		}
		List<Source> sources = new ArrayList<Source>();
		sources.add(source);
		entitymanager.getTransaction().commit();
		// ----------------------------------------------------------------------------------------
		
		importOrcidEntities(entitymanager, sources);		

		entitymanager.close();
		emfactory.close();
		
		emfactory = Persistence.createEntityManagerFactory(db);
		entitymanager = emfactory.createEntityManager();
		
		Source sourceORCID = getSourceByName(entitymanager, sourceName);
		List<Organization> orgs = retrieveOrganizations(entitymanager, source);
		
		entitymanager.getTransaction().begin();
		
		for (Organization org: orgs) {
    		    		
			// set unique identifier
    		String identifier =  sourceName + "::" + org.getLabel() + "::" + getPeopleIdentifiers(org);    		
			OrganizationIdentifier orgId = new OrganizationIdentifier();
			orgId.setIdentifier(identifier);
			orgId.setProvenance(sourceName);
			orgId.setIdentifierName("lid");
			orgId.setVisibility(false);
			orgId.setOrganization(org);
			entitymanager.persist(orgId);
    		
    	}
		
		entitymanager.getTransaction().commit();
		
		entitymanager.close();
		emfactory.close();

	}

}
