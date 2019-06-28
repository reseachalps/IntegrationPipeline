package it.unimore.alps.deduplication;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.management.RuntimeErrorException;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import javax.persistence.criteria.CriteriaBuilder.In;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.eclipse.persistence.jpa.jpql.parser.EntityOrValueExpressionBNF;

import it.unimore.alps.sql.model.AlternativeName;
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
import it.unimore.alps.sql.model.Publication;
import it.unimore.alps.sql.model.Source;
import it.unimore.alps.sql.model.SpinoffFrom;

public class DeduplicatorUtilTest {

	public static void main(String[] args) {

		boolean includeORCID = true;

		boolean FILTER_ORGANIZATION_CORRESPONDANCES = false;

		CommandLine commandLine;
		Option correspondenceOptionOrgs = Option.builder("correpondenceFileOrgs").hasArg().required(true)
				.desc("The file that contains organization correspondence. ").longOpt("correpondenceFileOrgs").build();

		Option correspondenceManualOptionOrgs = Option.builder("correpondenceManualFileOrgs").hasArg().required(true)
				.desc("The file that contains organization manual correspondence. ")
				.longOpt("correpondenceManualFileOrgs").build();

		Option correspondenceOptionPrjs = Option.builder("correpondenceFilePrjs").hasArg().required(true)
				.desc("The file that contains project correspondence. ").longOpt("correpondenceFilePrjs").build();

		Option sourceDBOption = Option.builder("sourceDB").hasArg().required(true).desc("Source DB. ")
				.longOpt("sourceDB").build();

		Option destinationDBOption = Option.builder("destinationDB").hasArg().required(true).desc("Destination DB. ")
				.longOpt("destinationDB").build();

		Options options = new Options();

		options.addOption(correspondenceOptionOrgs);
		options.addOption(correspondenceManualOptionOrgs);
		options.addOption(correspondenceOptionPrjs);
		options.addOption(sourceDBOption);
		options.addOption(destinationDBOption);

		CommandLineParser parser = new DefaultParser();

		String fileCorrespondenceOrgs = null;
		String fileCorrespondencePrjs = null;
		String fileCorrespondenceManualOrgs = null;
		String sourceDB = null;
		String destinationDB = null;
		try {
			commandLine = parser.parse(options, args);

			if (commandLine.hasOption("correpondenceFileOrgs")) {
				fileCorrespondenceOrgs = commandLine.getOptionValue("correpondenceFileOrgs");
			} else {
				System.out.println("Organization correspondences folder not provided. Use the orgFolder option.");
				System.exit(0);
			}

			if (commandLine.hasOption("correpondenceManualFileOrgs")) {
				fileCorrespondenceManualOrgs = commandLine.getOptionValue("correpondenceManualFileOrgs");
			} else {
				System.out
						.println("Organization  mnauelcorrespondences folder not provided. Use the orgFolder option.");
				System.exit(0);
			}

			System.out.println("Org file: " + fileCorrespondenceOrgs);
			if (commandLine.hasOption("correpondenceFilePrjs")) {
				fileCorrespondencePrjs = commandLine.getOptionValue("correpondenceFilePrjs");
			} else {
				System.out.println("Project correspondences folder not provided. Use the orgFolder option.");
				System.exit(0);
			}
			System.out.println("Prjs file: " + fileCorrespondencePrjs);

			if (commandLine.hasOption("sourceDB")) {
				sourceDB = commandLine.getOptionValue("sourceDB");
			} else {
				System.out.println("Source database is not provided. Use the sourceDB option.");
				System.exit(0);
			}
			System.out.println("Source database: " + sourceDB);

			if (commandLine.hasOption("destinationDB")) {
				destinationDB = commandLine.getOptionValue("destinationDB");
			} else {
				System.out.println("Destination database is not provided. Use the destinationDB option.");
				System.exit(0);
			}
			System.out.println("Destination database: " + destinationDB);

		} catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		DeduplicatorUtilTest dutil = new DeduplicatorUtilTest();

		int countOrganization = 0;

		// Set<Set<String>> correspondeces = dutil
		// .readCorrespondences("/Users/paolosottovia/Downloads/researchAlpsCsvs/correspondence.tsv");

		List<Organization> organizations = dutil.retrieveOrganizations(sourceDB, includeORCID);

		Set<String> allIds = new HashSet<>();
		allIds.addAll(dutil.retrieveAllOrganizationIds(organizations));

		Map<Integer, Organization> idOrganization = dutil
				.getMapOrganization(dutil.retrieveOrganizations(sourceDB, includeORCID));

		Map<String, Integer> LID_id = dutil.retrieveLIDOrganization(idOrganization);

		Set<Set<String>> correspondecesOrgs = dutil.readCorrespondences(fileCorrespondenceOrgs);

		System.out.println("Correspondences: ");
		for (Set<String> corr : correspondecesOrgs) {
			System.out.println("\t" + corr.toString());
		}

		if (FILTER_ORGANIZATION_CORRESPONDANCES) {

			List<String> organizationIds = new ArrayList<>();
			organizationIds.add("21874");

			Set<Set<String>> corrFiltered = new HashSet<>();

			for (Set<String> corr : correspondecesOrgs) {
				for (String id : organizationIds) {
					if (corr.contains(id)) {
						corrFiltered.add(corr);
					}

				}
			}

			correspondecesOrgs = corrFiltered;

			System.out.println("CORRESPONDECES FILTERED: ");
			for (Set<String> corr : correspondecesOrgs) {
				System.out.println("\t" + corr.toString());
			}

		}

		Set<Set<String>> correspondecesManualOrgs = dutil.readCorrespondencesManual(fileCorrespondenceManualOrgs,
				correspondecesOrgs, LID_id);

		Map<String, Set<String>> entityCorrespondeces = new HashMap<>();

		for (Set<String> corr : correspondecesOrgs) {
			for (String c : corr) {
				if (entityCorrespondeces.containsKey(c)) {
					// throw new RuntimeException("ERROR!!!");
				} else {
					entityCorrespondeces.put(c, corr);
				}
			}
		}

		for (Set<String> corr : correspondecesManualOrgs) {

			Set<String> newCorr = new HashSet<>();

			for (String cor : corr) {
				if (entityCorrespondeces.containsKey(cor)) {
					newCorr.addAll(entityCorrespondeces.get(cor));
					correspondecesOrgs.remove(entityCorrespondeces.get(cor));
				} else {
					if (!FILTER_ORGANIZATION_CORRESPONDANCES) {
						throw new RuntimeException("ERROR! missing entity!!!");
					}
				}
			}

			if (newCorr.size() != 0) {
				correspondecesOrgs.add(newCorr);
			}

			for (String c : newCorr) {
				entityCorrespondeces.put(c, newCorr);
			}

		}

		Set<Set<String>> correspondecesPrjs = dutil.readCorrespondences(fileCorrespondencePrjs);

		System.out.println("Number of organization correspondences: " + correspondecesOrgs.size());

		Set<String> allProjectIds = dutil.retrieveAllProjectIds(organizations);

		Map<Integer, Project> idProjects = dutil.getMapProject(dutil.retrieveProjects(sourceDB));

		Map<Integer, Integer> resolvedProject = dutil.resolveProjects(allProjectIds, correspondecesPrjs, idProjects);

		Map<Integer, Integer> resolvedOrganization = dutil.resolveOrganizations(correspondecesOrgs, idOrganization);

		Map<Integer, Source> idSources = dutil.getMapSource(dutil.retrieveSources(sourceDB));

		// TODO FIX

		Map<Integer, Publication> idPublications = dutil.getMapPublications(dutil.retrievePublications(sourceDB));

		Map<Integer, Person> idPerson = dutil.getMapPeople(dutil.retrievePeople(sourceDB));

		Map<String, OrganizationActivity> idOrganizationActivity = dutil
				.getMapOrganizationActivity(dutil.retrieveOrganizationActivity(sourceDB));

		// Resolve project

		// for (Map.Entry<Integer, Integer> entryProject : resolvedProject.entrySet()) {
		//
		// }

		System.out.println("RESOLVED ORGANIZATIONS:\n");

		for (Map.Entry<Integer, Integer> a : resolvedOrganization.entrySet()) {
			System.out.println("\t" + a.getKey() + "\t" + a.getValue());
		}

		System.out.println("END ORGANIZATIONS:\n");

		// System.exit(0);

		Map<Person, List<Publication>> personPublications = new HashMap<>();

		for (Entry<Integer, Publication> a : idPublications.entrySet()) {
			for (Person p : a.getValue().getAuthors()) {

				List<Project> prjUpdated = new ArrayList<>();
				Set<Integer> idProject = new HashSet<>();

				for (Project pr : a.getValue().getProjects()) {
					Integer idP = pr.getId();
					if (!idProject.contains(idP)) {
						Project pInserted = null;
						if (resolvedProject.containsKey(idP)) {

							pInserted = idProjects.get(idP);
							List<Source> sourceUpdated = new ArrayList<>();
							for (Source s : pInserted.getSources()) {
								sourceUpdated.add(idSources.get(s.getId()));
							}
							pInserted.setSources(sourceUpdated);

							prjUpdated.add(pInserted);
							idProject.add(idP);
						} else {
							List<Source> sourceUpdated = new ArrayList<>();
							for (Source s : p.getSources()) {
								sourceUpdated.add(idSources.get(s.getId()));
							}
							p.setSources(sourceUpdated);

							prjUpdated.add(pr);
							idProject.add(idP);
						}

					}

				}

				Publication pubUpdated = a.getValue();
				pubUpdated.setProjects(prjUpdated);

				if (personPublications.containsKey(p)) {

					personPublications.get(p).add(pubUpdated);

				} else {
					List<Publication> pubs = new ArrayList<>();

					pubs.add(pubUpdated);

					personPublications.put(p, pubs);
				}
			}
		}

		// PRINT

		System.out.println("\n\nSTATISTICs\n");
		System.out.println("Number of input organizations: " + idOrganization.size());
		System.out.println("Number of input projects: " + idProjects.size());
		System.out.println("Number of input sources: " + idSources.size());
		System.out.println("Number of input activityCode: " + idOrganizationActivity.size());

		System.out.println("\n\n END STATISTICs\n");

		Set<Integer> organizationToExclude = new HashSet<>();
		organizationToExclude.addAll(resolvedOrganization.keySet());
		organizationToExclude.addAll(resolvedOrganization.values());

		System.out.println("Number of organization to exclude: " + organizationToExclude.size());

		Set<Integer> idOrganizationNotDeduplicated = new HashSet<>();
		idOrganizationNotDeduplicated.addAll(idOrganization.keySet());
		idOrganizationNotDeduplicated.removeAll(organizationToExclude);

		System.out.println("Organization " + idOrganization.size());
		System.out.println("Organization not deduplicated: " + idOrganizationNotDeduplicated.size());

		// create complex case;
		Map<Integer, List<Integer>> reverseIdOrganization = new HashMap<>();
		for (Map.Entry<Integer, Integer> entry : resolvedOrganization.entrySet()) {
			Integer key = entry.getKey();
			Integer value = entry.getValue();

			if (key.equals(value)) {
				continue;
			}

			if (reverseIdOrganization.containsKey(value)) {
				reverseIdOrganization.get(value).add(key);
			} else {
				List<Integer> ids = new ArrayList<>();
				ids.add(key);
				reverseIdOrganization.put(value, ids);
			}
		}

		//
		System.out.println("Reverse ID organizations:\n");
		//
		for (Map.Entry<Integer, List<Integer>> a : reverseIdOrganization.entrySet()) {
			System.out.println(a.getKey() + "\t->\t" + a.getValue().toString());
		}

		//
		System.out.println("END Reverse ID organizations:\n");
		//
		Map<Integer, List<Integer>> idChildrens = new HashMap<>();

		Set<Integer> excludedIds = new HashSet<>();

		System.out.println("Number of representative: " + reverseIdOrganization.size());

		System.out.println("Number of organization deduplicated: " + resolvedOrganization.size());

		Set<Integer> projectIdsToInsert = new HashSet<>();

		Map<List<Integer>, List<Source>> childrenSources = new HashMap<>();

		for (Map.Entry<Integer, List<Integer>> it : reverseIdOrganization.entrySet()) {

			List<Integer> ids = new ArrayList<>();
			ids.add(it.getKey());
			for (Integer idRight : it.getValue()) {
				ids.add(idRight);
			}

			for (Integer idOrg : ids) {
				for (Project p : idOrganization.get(idOrg).getProjects()) {

					Integer idProject = resolvedProject.get(p.getId());
					if (idProject == null) {
						idProject = p.getId();
					}
					projectIdsToInsert.add(idProject);

				}
			}

		}

		for (Integer id : idOrganizationNotDeduplicated) {
			for (Project p : idOrganization.get(id).getProjects()) {

				Integer idProject = resolvedProject.get(p.getId());
				if (idProject == null) {
					idProject = p.getId();
				}
				projectIdsToInsert.add(idProject);

			}
		}

		for (Map.Entry<Integer, List<Integer>> it : reverseIdOrganization.entrySet()) {

			//
			System.out.println("------------------------------------------------------------------------------");

			System.out.println("ID: " + it.getKey() + "\t" + idOrganization.get(it.getKey()).getSources().toString());
			System.out.println("CHILDRENS: ");
			for (Organization children : idOrganization.get(it.getKey()).getChildrenOrganizations()) {
				System.out.println("\t\tchildren id: " + resolvedOrganization.get(children.getId()));
			}
			Organization ooo = idOrganization.get(it.getKey());
			if (ooo == null) {
				System.err.println("NULL ORGANIZATION: " + it.getKey() + "\t");
			}
			String lab = ooo.getLabel();

			System.out.println("Representative: " + it.getKey() + "\t" + idOrganization.get(it.getKey()).getLabel()
					+ "\t" + idOrganization.get(it.getKey()).getCity() + "\t" + "\n Others : \n");
			for (Integer id : it.getValue()) {

				System.out.println("\t" + id + "\t" + idOrganization.get(id).getLabel() + "\t"
						+ idOrganization.get(id).getCity() + "\t" + idOrganization.get(id).getSources().toString());
				System.out.println("\tCHILDRENS: ");
				for (Organization children : idOrganization.get(id).getChildrenOrganizations()) {
					System.out.println("\t\tchildren id: " + resolvedOrganization.get(children.getId()) + "\t\t"
							+ children.getId());
				}

			}
			System.out.println("\n------------------------------------------------------------------------------");
			//

			// Organization organization = idOrganization.get(it.getKey());

			List<Integer> organizationList = new ArrayList<>();
			organizationList.add(it.getKey());
			organizationList.addAll(it.getValue());

			// Set<Integer> organizationIds = new HashSet<>();
			Set<Integer> projectIds = new HashSet<>();
			Set<Integer> childrenOrganizationIds = new HashSet<>();
			Set<String> activitiesCodes = new HashSet<>();
			Set<Integer> sourceIds = new HashSet<>();

			List<AlternativeName> alternativeNames = new ArrayList<>();

			String Label = null;
			List<String> Acronyms = new ArrayList<>();
			String Address = null;
			String Alias = null;
			String City = null;
			String CityCode = null;
			String CommercialLabel = null;
			String Country = null;
			String CountryCode = null;
			Date CreationYear = null;
			Date FinancePrivateDate = null;
			String FinancePrivateEmployees = null;
			String IsPublic = "undefined";
			Float Lat = null;
			Float Lon = null;
			OrganizationType ORganizationType = null;
			String PostCode = null;
			String TypeCategoryCode = null;
			String TypeKind = null;
			String TypeLabel = null;
			String UrbanUnit = null;
			String UrbanUnitCode = null;

			Set<Badge> badges = new HashSet<>();
			List<Link> links = new ArrayList<>();
			Set<OrganizationExtraField> ExtraFields = new HashSet<>();

			Set<OrganizationIdentifier> Identifier = new HashSet<>();

			Set<OrganizationRelation> Relations = new HashSet<>();

			Set<Person> People = new HashSet<>();

			Set<Leader> Leaders = new HashSet<>();

			Set<Publication> Publications = new HashSet<>();
			Set<Source> Sources = new HashSet<>();
			Set<SpinoffFrom> SpinoffFroms = new HashSet<>();

			Set<Integer> AddressSources = new HashSet<>();
			Set<Integer> LeaderSources = new HashSet<>();

			Set<String> labelSources = new HashSet<>();

			for (Integer id : organizationList) {
				Organization org = idOrganization.get(id);

				List<Source> updatedSource1 = new ArrayList<>();
				for (Source s : org.getSources()) {
					updatedSource1.add(idSources.get(s.getId()));
					labelSources.add(s.getLabel());
				}

				AlternativeName aName = new AlternativeName();

				aName.setLabel(org.getLabel());
				aName.setSources(updatedSource1);

				alternativeNames.add(aName);

				for (OrganizationActivity act : org.getActivities()) {
					activitiesCodes.add(act.getCode());
				}

				for (Project p : org.getProjects()) {
					Integer pId = p.getId();
					if (resolvedProject.containsKey(pId)) {
						projectIds.add(resolvedProject.get(pId));
					} else {
						projectIds.add(pId);
					}

				}

				for (Badge badge : org.getBadges()) {
					badges.add(badge);
				}

				Set<Integer> localChildrenOrganizationIds = new HashSet<>();

				for (Organization orgChildren : org.getChildrenOrganizations()) {
					Integer idOrg = orgChildren.getId();

					if (resolvedOrganization.containsKey(idOrg)) {
						childrenOrganizationIds.add(resolvedOrganization.get(idOrg));
						localChildrenOrganizationIds.add(resolvedOrganization.get(idOrg));
					} else {
						childrenOrganizationIds.add(idOrg);
						localChildrenOrganizationIds.add(idOrg);
					}

				}

				for (Integer child_id : localChildrenOrganizationIds) {
					List<Integer> ids = new ArrayList<>();
					ids.add(it.getKey());
					ids.add(child_id);
					if (childrenSources.containsKey(ids)) {
						childrenSources.get(ids).addAll(org.getSources());
					} else {
						List<Source> sou = new ArrayList<>();
						sou.addAll(org.getSources());
						childrenSources.put(ids, sou);
					}
				}

				// Map<Link, List<Source>> linkSources = new HashMap<>();

				for (Link link : org.getLinks()) {

					List<Source> updatedSource = new ArrayList<>();
					for (Source s : link.getSources()) {
						updatedSource.add(idSources.get(s.getId()));
					}

					link.setSources(updatedSource);

					links.add(link);
				}

				List<OrganizationExtraField> extraFields = org.getOrganizationExtraFields();
				ExtraFields.addAll(extraFields);

				List<OrganizationIdentifier> identifier = org.getOrganizationIdentifiers();
				Identifier.addAll(identifier);

				List<OrganizationRelation> relations = org.getOrganizationRelations();
				Relations.addAll(relations);

				List<Person> people = org.getPeople();
				for (Person pers : people) {

					Person p = idPerson.get(pers.getId());

					People.add(p);
				}

				List<Source> addressSources = org.getAddressSources();

				for (Source sa : addressSources) {
					AddressSources.add(sa.getId());
				}

				List<Leader> leaders = org.getLeaders();
				Leaders.addAll(leaders);

				List<Publication> publications = new ArrayList<>();
				publications.addAll(org.getPublications());

				for (Person p : org.getPeople()) {
					if (personPublications.containsKey(p)) {
						publications.addAll(personPublications.get(p));
					}
				}

				for (Publication pub : publications) {

					Publication pubMod = idPublications.get(pub.getId());

					List<Source> updatedSource = new ArrayList<>();

					for (Source s : pub.getSources()) {
						updatedSource.add(idSources.get(s.getId()));
					}
					List<Person> authors = new ArrayList<>();

					for (Person per : pub.getAuthors()) {
						per.setSources(updatedSource);
						authors.add(per);
					}
					pubMod.setAuthors(authors);

					List<Project> prjUpdatedPub = new ArrayList<>();
					Set<Integer> idProjectPub = new HashSet<>();
					for (Project proj : pub.getProjects()) {
						Integer idP = proj.getId();
						if (!idProjectPub.contains(idP)) {

							Project pInserted = null;
							if (resolvedProject.containsKey(idP)) {

								pInserted = idProjects.get(idP);
								List<Source> sourceUpdated = new ArrayList<>();
								for (Source s : pInserted.getSources()) {
									sourceUpdated.add(idSources.get(s.getId()));
								}
								pInserted.setSources(sourceUpdated);

								prjUpdatedPub.add(pInserted);
								idProjectPub.add(idP);
							} else {
								List<Source> sourceUpdated = new ArrayList<>();
								for (Source s : proj.getSources()) {
									sourceUpdated.add(idSources.get(s.getId()));
								}
								proj.setSources(sourceUpdated);

								prjUpdatedPub.add(proj);
								idProjectPub.add(idP);
							}

						}
					}
					pubMod.setProjects(prjUpdatedPub);
					pubMod.setSources(updatedSource);

					Publications.add(pubMod);

				}

				List<Source> sources = org.getSources();
				Sources.addAll(sources);
				List<SpinoffFrom> spinoffFroms = org.getSpinoffFroms();
				SpinoffFroms.addAll(spinoffFroms);

				// String acronym = org.getAcronym();

				List<String> acronyms = org.getAcronyms();

				for (String acronym : acronyms) {
					if (!Acronyms.contains(acronym)) {
						Acronyms.add(acronym);
					}
				}

				String address = org.getAddress();
				if (Address == null) {
					Address = address;
				} else {
					if (Address.trim().equals("")) {
						if (address != null) {

							if (!address.trim().equals("")) {
								Address = address;
							}
						}
					}
				}
				String alias = org.getAlias();

				if (Alias == null) {
					Alias = alias;
				} else {
					if (Alias.trim().equals("")) {
						if (alias != null) {

							if (!alias.trim().equals("")) {
								Alias = alias;
							}
						}
					}
				}

				String city = org.getCity();
				if (City == null) {
					City = city;
				} else {
					if (City.trim().equals("")) {
						if (city != null) {

							if (!city.trim().equals("")) {
								City = city;
							}
						}
					}
				}

				String cityCode = org.getCityCode();
				if (CityCode == null) {
					CityCode = cityCode;
				} else {
					if (CityCode.trim().equals("")) {
						if (cityCode != null) {

							if (!cityCode.trim().equals("")) {
								CityCode = cityCode;
							}
						}
					}
				}
				String commercialLabel = org.getCommercialLabel();

				if (CommercialLabel == null) {
					CommercialLabel = commercialLabel;
				} else {
					if (CommercialLabel.trim().equals("")) {
						if (commercialLabel != null) {

							if (!commercialLabel.trim().equals("")) {
								CommercialLabel = commercialLabel;
							}
						}
					}
				}

				String country = org.getCountry();

				if (Country == null) {
					Country = country;
				} else {
					if (Country.trim().equals("")) {
						if (country != null) {

							if (!country.trim().equals("")) {
								Country = country;
							}
						}
					}
				}

				String countryCode = org.getCountryCode();

				if (CountryCode == null) {
					CountryCode = countryCode;
				} else {
					if (CountryCode.trim().equals("")) {
						if (countryCode != null) {

							if (!countryCode.trim().equals("")) {
								CountryCode = countryCode;
							}
						}
					}
				}

				Date creationYear = org.getCreationYear();
				if (CreationYear == null) {
					CreationYear = creationYear;
				}

				Date financePrivateDate = org.getFinancePrivateDate();
				if (FinancePrivateDate == null) {
					FinancePrivateDate = financePrivateDate;
				}

				String financePrivateEmployees = org.getFinancePrivateEmployees();

				if (FinancePrivateEmployees == null) {
					FinancePrivateEmployees = financePrivateEmployees;
				} else {
					if (FinancePrivateEmployees.trim().equals("")) {
						if (financePrivateEmployees != null) {

							if (!financePrivateEmployees.trim().equals("")) {
								FinancePrivateEmployees = financePrivateEmployees;
							}
						}
					}
				}

				String isPublic = org.getIsPublic();
				if (IsPublic.equals("undefined")) {

					if (!isPublic.equals("undefined")) {
						IsPublic = isPublic;
					}
				}
				String label = org.getLabel();
				if (Label == null) {
					Label = label;
				} else {
					if (Label.trim().equals("")) {
						if (label != null) {

							if (!label.trim().equals("")) {
								Label = label;
							}
						}
					}
				}

				Float lat = org.getLat();
				if (Lat == null) {
					if (lat != null) {
						if (!lat.equals(0.0f)) {
							Lat = lat;
						}
					}
				}
				Float lon = org.getLon();
				if (Lon == null) {
					if (lon != null) {
						if (!lon.equals(0.0f)) {
							Lon = lon;
						}
					}
				}
				OrganizationType organizationType = org.getOrganizationType();
				if (ORganizationType == null) {
					ORganizationType = organizationType;
				}
				String postCode = org.getPostcode();
				if (PostCode == null) {
					PostCode = postCode;
				} else {
					if (PostCode.trim().equals("")) {
						if (postCode != null) {
							if (!postCode.trim().equals("")) {
								PostCode = postCode;
							}
						}
					}
				}

				String typeCategoryCode = org.getTypeCategoryCode();
				if (TypeCategoryCode == null) {
					TypeCategoryCode = typeCategoryCode;
				} else {
					if (TypeCategoryCode.trim().equals("")) {
						if (typeCategoryCode != null) {
							if (!typeCategoryCode.trim().equals("")) {
								TypeCategoryCode = typeCategoryCode;
							}
						}
					}
				}
				String typeKind = org.getTypeKind();

				if (TypeKind == null) {
					TypeKind = typeKind;
				} else {
					if (TypeKind.trim().equals("")) {
						if (typeKind != null) {
							if (!typeKind.trim().equals("")) {
								TypeKind = typeKind;
							}
						}
					}
				}
				String typeLabel = org.getTypeLabel();
				if (TypeLabel == null) {
					TypeLabel = typeLabel;
				} else {
					if (TypeLabel.trim().equals("")) {
						if (typeLabel != null) {
							if (!typeLabel.trim().equals("")) {
								TypeLabel = typeLabel;
							}
						}
					}
				}
				String urbanUnit = org.getUrbanUnit();

				if (UrbanUnit == null) {
					UrbanUnit = urbanUnit;
				} else {
					if (UrbanUnit.trim().equals("")) {
						if (urbanUnit != null) {
							if (!urbanUnit.trim().equals("")) {
								UrbanUnit = urbanUnit;
							}
						}
					}
				}

				String urbanUnitCode = org.getUrbanUnitCode();

				if (UrbanUnitCode == null) {
					UrbanUnitCode = urbanUnitCode;
				} else {
					if (UrbanUnitCode.trim().equals("")) {
						if (urbanUnitCode != null) {
							if (!urbanUnitCode.trim().equals("")) {
								UrbanUnitCode = urbanUnitCode;
							}
						}
					}
				}

				for (Source s : org.getSources()) {
					sourceIds.add(s.getId());
				}

			}

			System.out.println("ALTERNATIVE NAMES");

			for (AlternativeName aname : alternativeNames) {
				System.out.println("\t" + aname.getLabel());

			}

			System.out.println("\n\n");

			Organization org = new Organization();
			// test
			org.setId(it.getKey());
			org.setAcronyms(Acronyms);

			Map<String, List<AlternativeName>> nameAlternativeName = new HashMap<>();
			for (AlternativeName aName : alternativeNames) {
				String name = aName.getLabel().toLowerCase().trim();
				if (nameAlternativeName.containsKey(name)) {
					nameAlternativeName.get(name).add(aName);

				} else {
					List<AlternativeName> names = new ArrayList<>();
					names.add(aName);
					nameAlternativeName.put(name, names);
				}
			}

			List<AlternativeName> alternativeNamesList = new ArrayList<>();

			for (Map.Entry<String, List<AlternativeName>> entry : nameAlternativeName.entrySet()) {

				AlternativeName al = new AlternativeName();

				// al.setLabel(entry.getKey());
				List<Source> sources = new ArrayList<>();
				Set<Integer> source_Ids = new HashSet();

				for (AlternativeName aName : entry.getValue()) {
					al.setLabel(aName.getLabel());
					// sources.addAll(aName.getSources());
					for (Source sa : aName.getSources()) {
						source_Ids.add(sa.getId());
					}

				}

				for (Integer sid : source_Ids) {
					sources.add(idSources.get(sid));
				}

				al.setSources(sources);
				alternativeNamesList.add(al);

			}

			org.setAlternativeNames(alternativeNamesList);

			List<OrganizationActivity> oa = new ArrayList<>();
			for (String code : activitiesCodes) {
				oa.add(idOrganizationActivity.get(code));
			}
			org.setActivities(oa);
			org.setAddress(Address);
			org.setAlias(Alias);

			// org.setBadges(badges);
			List<Badge> badgeList = new ArrayList<>();
			for (Badge b : badges) {
				badgeList.add(b);
			}
			org.setBadges(badgeList);

			// org.setChildrenOrganizations(childrenOrganizations);
			List<Organization> childrenOrganization = new ArrayList<>();
			for (Integer id : childrenOrganizationIds) {
				childrenOrganization.add(idOrganization.get(id));
			}
			org.setChildrenOrganizations(childrenOrganization);
			List<Integer> childrenIds = new ArrayList<>();
			for (Organization OOO : org.getChildrenOrganizations()) {
				if (!childrenIds.contains(OOO.getId())) {
					childrenIds.add(OOO.getId());
				}
			}
			if (childrenIds.size() > 0) {
				if (idChildrens.containsKey(it.getKey())) {
					idChildrens.get(it.getKey()).addAll(childrenIds);

				} else {
					idChildrens.put(it.getKey(), childrenIds);
				}

			}

			org.setCity(City);
			org.setCityCode(CityCode);
			org.setCommercialLabel(CommercialLabel);
			org.setCountry(Country);
			org.setCountryCode(CountryCode);
			org.setCreationYear(CreationYear);
			org.setFinancePrivateDate(FinancePrivateDate);
			org.setFinancePrivateEmployees(FinancePrivateEmployees);
			org.setIsPublic(IsPublic);
			org.setLabel(Label);
			if (Lat != null) {
				org.setLat(Lat);
			}
			List<Leader> leaderList = new ArrayList<>();
			for (Integer sid : LeaderSources) {

			}

			for (Leader l : Leaders) {
				List<Source> sourcesUpdated = new ArrayList<>();
				for (Source s : l.getSources()) {
					sourcesUpdated.add(idSources.get(s.getId()));
				}

				l.setSources(sourcesUpdated);
				leaderList.add(l);

			}
			org.setLeaders(leaderList);
			List<Link> linkList = new ArrayList<>();

			Map<Link, Set<Source>> linkSources = new HashMap<>();

			Set<String> urls = new HashSet<>();

			for (Link l : links) {
				if (l.getUrl() != null) {
					String urlCleaned = l.getUrl().trim(); // use a better cleaning strategy

					urlCleaned = l.getUrl().trim();
					if (!urls.contains(urlCleaned)) {
						linkList.add(l);
						urls.add(urlCleaned);
					}

					List<Source> sourcesUpdated = new ArrayList<>();
					for (Source s : l.getSources()) {
						sourcesUpdated.add(idSources.get(s.getId()));
					}

					l.setSources(sourcesUpdated);

					if (linkSources.containsKey(l)) {
						linkSources.get(l).addAll(sourcesUpdated);
					} else {
						Set<Source> ss = new HashSet<>();
						ss.addAll(sourcesUpdated);
						linkSources.put(l, ss);
					}

				}
			}

			List<Link> ll = new ArrayList<>();

			for (Map.Entry<Link, Set<Source>> entry : linkSources.entrySet()) {

				Link l = entry.getKey();

				List<Source> sources = new ArrayList<>();
				// sources.addAll(l.getSources());
				sources.addAll(entry.getValue());

				l.setSources(sources);
				ll.add(l);

			}

			org.setLinks(ll);
			if (Lon != null) {
				org.setLon(Lon);
			}

			List<Source> sourceList = new ArrayList<>();
			for (Integer id : sourceIds) {
				sourceList.add(idSources.get(id));
			}
			org.setSources(sourceList);

			org.setTypeCategoryCode(TypeCategoryCode);
			org.setTypeKind(TypeKind);
			org.setTypeLabel(TypeLabel);
			org.setUrbanUnit(UrbanUnit);
			org.setUrbanUnitCode(UrbanUnitCode);

			// org.setOrganizationType(ORganizationType);

			// LAST UPDATE IS HERE

			List<Person> peopleList = new ArrayList<>();
			// peopleList.addAll(People);

			for (Person p : People) {
				List<Source> sourceUpdated = new ArrayList<>();
				for (Source s : p.getSources()) {

					sourceUpdated.add(idSources.get(s.getId()));

				}
				p.setSources(sourceUpdated);

				peopleList.add(p);
			}
			//
			org.setPeople(resolvePeople(peopleList));
			org.setPostcode(PostCode);
			List<Source> AddressSourcesList = new ArrayList<>();

			for (Integer sid : AddressSources) {
				AddressSourcesList.add(idSources.get(sid));
			}

			System.out.println("ADDRESS SOURCES: ");
			printSources(AddressSourcesList);

			org.setAddressSources(AddressSourcesList);

			List<Project> prjList = new ArrayList<>();
			for (Integer id : projectIds) {

				Project project = idProjects.get(id);
				if (project != null) {
					List<Source> sourceUpdated = new ArrayList<>();
					if (project.getSources() != null) {
						for (Source s : project.getSources()) {
							sourceUpdated.add(idSources.get(s.getId()));
						}
					}
					project.setSources(sourceUpdated);

					prjList.add(project);
				} else {
					System.err.println("ERROR");
					System.err.println("NULL PROJECT!!!!! ID: " + id);
					System.err.println("ERROR");

				}
			}
			org.setProjects(prjList);
			List<Publication> publicationList = new ArrayList<>();
			publicationList.addAll(Publications);
			Set<Integer> publicationsIds = new HashSet<>();
			List<Publication> publicationsFiltered = new ArrayList<>();
			for (Publication p : Publications) {
				Integer id = p.getId();

				if (publicationsIds.contains(id)) {
					System.err.println("DUPLICATE PUBLICATION ID!!!!");
				} else {
					publicationsFiltered.add(p);
					publicationsIds.add(id);
				}

			}

			org.setPublications(publicationsFiltered);

			boolean excludeOrganization = false;

			Set<String> labelSources_1 = new HashSet<>();
			for (Source s : org.getSources()) {
				labelSources_1.add(s.getLabel().toLowerCase());
			}

			if (labelSources_1.size() == 1) {
				if (labelSources_1.contains("orcid")) {
					excludeOrganization = true;
				}
			}

			if (!excludeOrganization) {

				List<Integer> CCC = new ArrayList<>();
				for (Organization OOO : org.getChildrenOrganizations()) {
					Integer oid = OOO.getId();

					if (resolvedOrganization.containsKey(OOO.getId())) {
						oid = resolvedOrganization.get(OOO.getId());
					}

					if (!CCC.contains(oid)) {
						CCC.add(oid);
					}
				}
				if (CCC.size() > 0) {
					idChildrens.put(it.getKey(), CCC);
				}

				countOrganization++;
			} else {
				excludedIds.add(it.getKey());
				System.out.println("Label: " + org.getLabel() + "\tEXCLUDED");

			}

			// NEW ENTRY
			org.getChildrenOrganizations().clear();

			// INSERT OBJECT THAT CONTAINS THE ORGANIZATION ITSELF

			List<SpinoffFrom> spins = new ArrayList<>();
			spins.addAll(SpinoffFroms);
			for (SpinoffFrom sp : spins) {
				sp.setOrganization(org);
			}
			org.setSpinoffFroms(spins);

			List<OrganizationExtraField> organizationExtraFieldsList = new ArrayList<>();
			organizationExtraFieldsList.addAll(ExtraFields);
			for (OrganizationExtraField orgEF : organizationExtraFieldsList) {
				orgEF.setOrganization(org);
			}

			org.setOrganizationExtraFields(organizationExtraFieldsList);

			List<OrganizationIdentifier> organizationIdentifiersList = new ArrayList<>();
			organizationIdentifiersList.addAll(Identifier);

			for (OrganizationIdentifier oid : organizationIdentifiersList) {
				oid.setOrganization(org);
			}

			org.setOrganizationIdentifiers(organizationIdentifiersList);

			List<OrganizationRelation> organizationRelationsList = new ArrayList<>();
			organizationRelationsList.addAll(Relations);
			for (OrganizationRelation orgR : organizationRelationsList) {
				orgR.setOrganization(org);
			}

			org.setOrganizationRelations(organizationRelationsList);

			// REMOVE HERE
			// org.getPublications().clear();

			idOrganization.put(it.getKey(), org);

			System.out.println("Label: " + org.getLabel());

			System.out.println("Number of childrens: " + org.getChildrenOrganizations().size());
			for (Organization o : org.getChildrenOrganizations()) {
				System.out.println("\t" + o.getId());
			}
			System.out.println("\n");

		}

		Set<Integer> uniqueIds = new HashSet<>();

		// update map with entities;
		Set<Integer> idDeDuplicated = new HashSet<>();
		idDeDuplicated.addAll(reverseIdOrganization.keySet());
		System.out.println(
				"EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE");
		System.out.println("Id DUPLICATED");
		for (Integer id : idDeDuplicated) {
			System.out.print(id + "\t");
		}
		System.out.println("\n\n");
		System.out.println("Id NOT DUPLICATED");
		for (Integer id : idOrganizationNotDeduplicated) {
			System.out.print(id + "\t");
		}
		System.out.println(
				"EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE");

		//
		//
		///
		////
		/////
		//////
		// System.out.println("EXIT BEFORE");
		// System.exit(-1);
		//////
		/////
		////
		///
		//

		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(destinationDB);

		EntityManager entitymanager = emfactory.createEntityManager();

		System.out.println("Number of deduplicated entities to INSERT: " + idDeDuplicated.size());

		entitymanager.getTransaction().begin();

		// persist sources

		for (Integer id : idSources.keySet()) {
			Source s = idSources.get(id);
			entitymanager.detach(s);
			entitymanager.persist(s);
			idSources.put(id, s);
			System.out.println("id: " + id + "\t" + s.getId());
		}

		// entitymanager.getTransaction().commit();
		// entitymanager.close();
		// System.out.println("EXIT");
		// System.exit(0);

		// resolve project and persist them

		System.out.println("PERSIST PROJECTs ");

		for (Integer idProject : projectIdsToInsert) {
			Project p = idProjects.get(idProject);
			if (p == null) {
				throw new RuntimeException("PROJECT is NULL!!!");
			}
			List<Source> sources = new ArrayList<>();
			for (Source s : p.getSources()) {
				sources.add(idSources.get(s.getId()));

			}
			p.setSources(sources);

			idProjects.put(idProject, p);

			// persist project
			System.out.println("Persist project with id: " + idProject);
			entitymanager.persist(p);

			if (!entitymanager.contains(p)) {
				throw new RuntimeException("PROJECT id: " + p.getId() + " IS NOT PERSISTED!!!");
			}
		}

		System.out.println("PROJECT INSERTION PHASE DONE. Number of project inserted: " + projectIdsToInsert.size());

		System.out.println("Children Sources");
		System.out.println("Number of entries: " + childrenSources.size());

		for (Map.Entry<List<Integer>, List<Source>> entry : childrenSources.entrySet()) {

			String sourceString = "";

			for (Source s : entry.getValue()) {
				sourceString += s.getLabel() + "\t";
			}

			System.out.println(entry.getKey().get(0) + " -> " + entry.getKey().get(1) + "\t\tSources: " + sourceString);
		}

		System.out.println("\n\n\n\n");

		System.out.println("Number of companies to insert in the first step: " + idDeDuplicated.size());

		List<Integer> idDeDuplicatedSorted = new ArrayList<>();
		idDeDuplicatedSorted.addAll(idDeDuplicated);
		Collections.sort(idDeDuplicatedSorted);

		for (Integer id : idDeDuplicatedSorted) {

			if (uniqueIds.contains(id)) {
				System.err.println("Duplicate ID: " + id);
			} else {
				uniqueIds.add(id);
			}

			System.out.println("----------------------------------------------------------------");
			System.out.println("ID: " + id);

			Organization org = idOrganization.get(id);
			// entitymanager.detach(org);
			List<Source> sourceUpdated = new ArrayList<>();

			for (Source s : org.getSources()) {
				sourceUpdated.add(idSources.get(s.getId()));
			}
			org.setSources(sourceUpdated);

			List<Source> addressSources = org.getAddressSources();

			Set<Source> AddressSources = new HashSet<>();
			for (Source sa : addressSources) {
				AddressSources.add(idSources.get(sa.getId()));
			}

			List<Project> projectUpdated = new ArrayList<>();
			List<Integer> idsPrj = new ArrayList<>();
			for (Project p : org.getProjects()) {

				Project pp = idProjects.get(p.getId());
				if (pp == null) {
					throw new RuntimeException("PROJECT is NULL!!!");
				}
				if (!idsPrj.contains(pp.getId())) {
					projectUpdated.add(pp);
				}
				idsPrj.add(p.getId());

			}

			org.setProjects(projectUpdated);

			// org.getOrganizationRelations().clear();
			// List<OrganizationRelation> organizationRelationUpdated = new ArrayList<>();
			// for (OrganizationRelation or : org.getOrganizationRelations()) {
			// or.setOrganization(org);
			// }
			//
			// // org.getSpinoffFroms().clear();
			//
			// List<SpinoffFrom> spinOffFromUpdated = new ArrayList<>();
			// for (SpinoffFrom sp : org.getSpinoffFroms()) {
			// sp.setOrganization(org);
			// }

			// UPDATE HERE
			org.getActivities().clear();
			org.getBadges().clear();
			org.getBadges().clear();

			Set<Person> people = new HashSet<>();
			people.addAll(org.getPeople());

			List<Person> peopleList = new ArrayList<>();
			peopleList.addAll(people);

			org.setPeople(resolvePeople(peopleList));

			boolean excludeOrganization = false;

			Set<String> labelSources = new HashSet<>();
			for (Source s : org.getSources()) {
				labelSources.add(s.getLabel().toLowerCase());
			}

			if (labelSources.size() == 1) {
				if (labelSources.contains("orcid")) {
					excludeOrganization = true;
				}
			}

			if (!excludeOrganization) {

				List<Integer> childrenIds = new ArrayList<>();
				for (Organization OOO : org.getChildrenOrganizations()) {
					Integer oid = OOO.getId();

					if (resolvedOrganization.containsKey(OOO.getId())) {
						oid = resolvedOrganization.get(OOO.getId());
					}

					if (!childrenIds.contains(oid)) {
						childrenIds.add(oid);
					}
				}
				if (childrenIds.size() > 0) {
					idChildrens.put(id, childrenIds);
				}

				insertOrganization(entitymanager, org, id, idOrganization);
				System.out.println("Label: " + org.getLabel() + "\tINSERTED");

				countOrganization++;
			} else {
				excludedIds.add(id);
				System.out.println("Label: " + org.getLabel() + "\tEXCLUDED");

			}

		}

		int count = 0;
		for (List<Integer> III : idChildrens.values()) {
			count += III.size();
		}

		System.out.println("Number of organization with children: " + idChildrens.size() + "\tCHILDRENS: " + count);

		// System.out.println("EXIT BEFORE");
		// entitymanager.getTransaction().commit();
		// entitymanager.close();
		// emfactory.close();
		// System.exit(0);

		System.out.println("Insertion of complex cases!!!!");

		System.out.println("Number of companies to insert in the second step: " + idOrganizationNotDeduplicated.size());
		// entitymanager.getTransaction().begin();

		for (Integer id : idDeDuplicated) {
			if (idOrganizationNotDeduplicated.contains(id)) {
				throw new RuntimeException("Wrong id!!!");
			}
		}

		// insert simple organization
		for (Integer id : idOrganizationNotDeduplicated) {

			if (uniqueIds.contains(id)) {
				System.err.println("Duplicate ID: " + id);
			} else {
				uniqueIds.add(id);
			}

			System.out.println("----------------------------------------------------------------");
			System.out.println("INSERTED ID: " + id);
			Organization organization = idOrganization.get(id);

			List<Organization> children = organization.getChildrenOrganizations();
			List<Organization> childrenUpdated = new ArrayList<>();
			Set<Integer> idChildren = new HashSet<>();

			for (Organization org : children) {
				Integer idOrg = org.getId();
				boolean resolved = false;
				if (!idChildren.contains(idOrg)) {

					Organization orgChild = null;
					if (resolvedOrganization.containsKey(idOrg)) {
						idChildren.add(resolvedOrganization.get(idOrg));

						orgChild = idOrganization.get(resolvedOrganization.get(idOrg));
						resolved = true;
						// childrenUpdated.add(orgChild);
					} else {
						idChildren.add(idOrg);
						orgChild = idOrganization.get(idOrg);

					}

					if (orgChild == null) {
						System.err.println("Null child");
						System.err.println("Original ID: " + idOrg);
						System.err.println("Resolved: " + resolved);

						System.exit(-1);
					}

					childrenUpdated.add(orgChild);
				}
			}

			organization.setChildrenOrganizations(childrenUpdated);

			List<Integer> childrenIds = new ArrayList<>();
			for (Organization OOO : organization.getChildrenOrganizations()) {

				if (OOO == null) {
					System.err.println("Null organization");
					System.err.println("Parent: " + organization.getLabel());
					System.exit(-1);

				}

				if (!childrenIds.contains(OOO.getId())) {
					childrenIds.add(OOO.getId());
				}
			}

			if (childrenIds.size() > 0) {
				idChildrens.put(id, childrenIds);
			}

			// CHANGE HERE
			// HERE
			organization.getChildrenOrganizations().clear();

			// List<OrganizationActivity> emptyOrganizationActivities = new ArrayList<>();
			organization.getActivities().clear();
			organization.getBadges().clear();

			if (organization == null) {
				throw new RuntimeException("Err");
			}

			for (Link link : organization.getLinks()) {
				if (link != null) {
					String url = link.getUrl();
					if (url != null) {
						if (!url.contains("http://") && !url.contains("https://")) {
							link.setUrl("http://" + url);
						}
					}

					List<Source> updatedSource = new ArrayList<>();
					for (Source s : link.getSources()) {
						updatedSource.add(idSources.get(s.getId()));
					}

					link.setSources(updatedSource);
				}

			}

			// resolve projects
			List<Project> prj = organization.getProjects();
			List<Project> prjUpdated = new ArrayList<>();
			Set<Integer> idProject = new HashSet<>();

			for (Project p : prj) {
				Integer idP = p.getId();
				// MODIFIED HERE
				Integer idResolved = resolvedProject.get(idP);
				if (idResolved == null) {
					idResolved = idP;
				}

				Project pp = idProjects.get(idResolved);
				if (pp == null) {
					throw new RuntimeException("PROJECT is NULL!!!");
				}
				if (!idProject.contains(idResolved)) {
					prjUpdated.add(pp);
				}
				idProject.add(idResolved);

			}

			organization.setProjects(prjUpdated);
			// MANAGER PERSIST
			List<Integer> sourceIds = new ArrayList<>();
			for (Source s : organization.getSources()) {
				sourceIds.add(s.getId());
			}
			List<Source> sources = new ArrayList<>();
			for (Integer sId : sourceIds) {
				sources.add(idSources.get(sId));
			}

			Set<Person> People = new HashSet<>();

			for (Person pers : organization.getPeople()) {

				Person p = idPerson.get(pers.getId());
				List<Source> updatedSource = new ArrayList<>();
				for (Source s : p.getSources()) {
					updatedSource.add(idSources.get(s.getId()));
				}

				p.setSources(updatedSource);

				List<PersonIdentifier> pIds = new ArrayList<>();

				pIds.addAll(p.getPersonIdentifiers());
				for (PersonIdentifier id_person : pIds) {
					id_person.setPerson(p);
				}
				p.setPersonIdentifiers(pIds);

				People.add(p);
			}

			List<Person> PeopleList = new ArrayList<>();
			PeopleList.addAll(People);

			organization.setPeople(resolvePeople(PeopleList));

			List<Leader> Leaders = new ArrayList<>();
			for (Leader l : organization.getLeaders()) {

				List<Source> updatedSource = new ArrayList<>();
				for (Source s : l.getSources()) {
					updatedSource.add(idSources.get(s.getId()));
				}

				l.setSources(updatedSource);
				Leaders.add(l);

			}
			organization.setLeaders(Leaders);

			List<Publication> publications = organization.getPublications();
			Set<Publication> Publications = new HashSet<>();
			for (Publication pub : publications) {

				Publication pubMod = idPublications.get(pub.getId());

				List<Source> updatedSource = new ArrayList<>();

				for (Source s : pub.getSources()) {
					updatedSource.add(idSources.get(s.getId()));
				}

				List<Person> authors = new ArrayList<>();

				for (Person per : pub.getAuthors()) {
					per.setSources(updatedSource);
					authors.add(per);

				}

				List<Project> prjUpdatedPub = new ArrayList<>();
				Set<Integer> idProjectPub = new HashSet<>();
				for (Project proj : pub.getProjects()) {
					Integer idP = proj.getId();
					Integer idPp = resolvedProject.get(idP);
					Project p = idProjects.get(idPp);

					if (p == null) {
						throw new RuntimeException("NULL project");
					}
					if (!idProject.contains(idPp)) {
						prjUpdatedPub.add(p);
					}
					idProject.add(idPp);
				}
				pubMod.setProjects(prjUpdatedPub);
				pubMod.setSources(updatedSource);
				pubMod.setAuthors(authors);
				Publications.add(pubMod);

			}
			organization.setPeople(resolvePeople(PeopleList));
			organization.setLeaders(Leaders);

			List<Publication> publicationList = new ArrayList<>();
			publicationList.addAll(Publications);

			organization.setPublications(publicationList);

			// REMOVE HERE
			// organization.getPublications().clear();

			organization.setAddressSources(sources);
			organization.setSources(sources);

			/// exclude only orcid organization
			Set<String> labelSources = new HashSet<>();
			for (Source s : organization.getSources()) {
				labelSources.add(s.getLabel().toLowerCase());
			}

			boolean excludeOrganization = false;

			if (labelSources.size() == 1) {
				if (labelSources.contains("orcid")) {
					excludeOrganization = true;
				}
			}

			// INSERT OBJECT THAT CONTAINS THE ORGANIZATION ITSELF

			List<SpinoffFrom> spins = new ArrayList<>();
			spins.addAll(organization.getSpinoffFroms());
			for (SpinoffFrom sp : spins) {
				sp.setOrganization(organization);
			}
			organization.setSpinoffFroms(spins);

			List<OrganizationExtraField> organizationExtraFieldsList = new ArrayList<>();
			organizationExtraFieldsList.addAll(organization.getOrganizationExtraFields());
			for (OrganizationExtraField orgEF : organizationExtraFieldsList) {
				orgEF.setOrganization(organization);
			}

			organization.setOrganizationExtraFields(organizationExtraFieldsList);

			List<OrganizationIdentifier> organizationIdentifiersList = new ArrayList<>();
			organizationIdentifiersList.addAll(organization.getOrganizationIdentifiers());

			for (OrganizationIdentifier oid : organizationIdentifiersList) {
				oid.setOrganization(organization);
			}

			organization.setOrganizationIdentifiers(organizationIdentifiersList);

			List<OrganizationRelation> organizationRelationsList = new ArrayList<>();
			organizationRelationsList.addAll(organization.getOrganizationRelations());
			for (OrganizationRelation orgR : organizationRelationsList) {
				orgR.setOrganization(organization);
			}

			organization.setOrganizationRelations(organizationRelationsList);

			if (!excludeOrganization) {

				System.out.println("Label: " + organization.getLabel() + "\tINSERTED");
				//
				//

				insertOrganization(entitymanager, organization, id, idOrganization);
				idOrganization.put(id, organization);
				countOrganization++;
				if (childrenIds.size() > 0) {
					idChildrens.put(id, childrenIds);
				}

			} else {

				excludedIds.add(id);
				System.out.println("Label: " + organization.getLabel() + "\tEXCLUDED");

			}

			// insertOrganization(entitymanager, organization, id, idOrganization);

		}

		System.out.println("Number of inserted organization: " + countOrganization);

		//
		// System.out.println("EXIT BEFORE");
		// entitymanager.getTransaction().commit();
		// entitymanager.close();
		// emfactory.close();
		//
		// System.exit(0); // /REMOVE HERE

		// merge
		System.out.println("\n\nMERGE");
		System.out.println();

		System.out.println("Number of element of organizationMap: " + idOrganization.size());

		System.out.println("KEYS: ===========================================================");
		System.out.println("" + idOrganization.keySet().toString());
		System.out.println("KEYS: ===========================================================");

		for (Integer id : idOrganizationNotDeduplicated) {
			mergeOrganization(entitymanager, idOrganization, idSources, idChildrens, id, excludedIds, childrenSources);
		}

		System.out.println("\n\n\n\n");

		// Test MERGE ENTITY
		// entitymanager.getTransaction().commit();
		// entitymanager.close();
		// emfactory.close();
		// System.out.println("EXIT BEFORE!!");
		// System.exit(-1);

		System.out.println("ORGANIZATION DEDUPLICATED!!!");
		System.out.println("\n\n\n\n");
		for (Integer id : idDeDuplicated) {
			mergeOrganization(entitymanager, idOrganization, idSources, idChildrens, id, excludedIds, childrenSources);
		}

		System.out.println("END FOR ORGANIZATION");
		//
		// PUBLICATIONS

		// if (includeORCID) {
		// for (Publication p : idPublications.values()) {
		//
		// if (!entitymanager.contains(p)) {
		//
		// List<Source> updatedSource = new ArrayList<>();
		//
		// for (Source s : p.getSources()) {
		// updatedSource.add(idSources.get(s.getId()));
		// }
		//
		// List<Person> authors = new ArrayList<>();
		//
		// for (Person per : p.getAuthors()) {
		// per.setSources(updatedSource);
		//
		// authors.add(per);
		// }
		//
		// p.setAuthors(authors);
		// p.setSources(updatedSource);
		//
		// entitymanager.persist(p);
		// }
		// }
		// }

		entitymanager.getTransaction().commit();
		entitymanager.close();
		emfactory.close();

		// insert complex organization

		/// DELETE
		//
		//
		//
		//

	}

	private Map<String, Integer> retrieveLIDOrganization(Map<Integer, Organization> idOrganization) {

		Map<String, Integer> LID_id = new HashMap<>();

		for (Map.Entry<Integer, Organization> entry : idOrganization.entrySet()) {

			List<OrganizationIdentifier> organizationIdentifiers = entry.getValue().getOrganizationIdentifiers();

			for (OrganizationIdentifier id : organizationIdentifiers) {
				if (id.getIdentifierName().equals("lid")) {
					String lid = id.getIdentifier();
					if (LID_id.containsKey(lid)) {

						System.err.println("");

						Integer orgID = LID_id.get(lid);

						System.err.println("LID: " + lid);
						System.err.println("OLD ID: " + orgID);
						System.err.println("NEW ID: " + entry.getKey());

						throw new RuntimeException("ERRORE DUPLICATE LID");
					} else {
						LID_id.put(lid, entry.getValue().getId());
					}
				}
			}

		}

		return LID_id;
	}

	private static void printSources(List<Source> sources) {
		for (Source s : sources) {
			System.out.println(s.toString());
		}
	}

	private static void mergeOrganization(EntityManager entitymanager, Map<Integer, Organization> idOrganization,
			Map<Integer, Source> idSources, Map<Integer, List<Integer>> idChildrens, Integer id,
			Set<Integer> excludeIds, Map<List<Integer>, List<Source>> childrenSources) {
		System.out.println("-----------------------------------------------------------------------");

		Organization org = idOrganization.get(id);
		System.out.println("ID: " + id + "\t" + org.getLabel());
		Set<Integer> idChildrenIDS = new HashSet<>();
		if (idChildrens.containsKey(id)) {
			idChildrenIDS.addAll(idChildrens.get(id));
		}
		List<Organization> orgs = new ArrayList<>();
		System.out.println("\tNumber of Children: " + idChildrenIDS.size());
		System.out.println("Childrens: " + idChildrenIDS.toString());

		List<Person> peopleOrg = org.getPeople();

		for (Integer idC : idChildrenIDS) {
			// exclude orcid
			if (excludeIds.contains(idC)) {
				System.out.println("EXCLUDE CHILDREN ID: " + idC);
				continue;
			}

			if (idC.intValue() == id.intValue()) {
				System.out.println("parent structure equals to structure itself.");
				continue;
			}

			if (!idOrganization.containsKey(idC)) {
				System.err.println("MAP DOES NOT CONTAINS the organization!!!!");
				continue;
			}

			Organization orgCC = idOrganization.get(idC);
			if (orgCC == null) {
				System.err.println("THE ORGANIZATION IS NULL!!!!");
				continue;

			}

			List<Integer> childrenPair = new ArrayList<>();
			childrenPair.add(id);
			childrenPair.add(idC);

			if (childrenSources.containsKey(childrenPair)) {

				List<Source> sources = childrenSources.get(childrenPair);
				if (sources.size() < 3) {

					if (sources.size() == 2) {

						String sourceLabel1 = sources.get(0).getLabel();
						String sourceLabel2 = sources.get(1).getLabel();

						String sourceLabel = "";

						if (sourceLabel1.equals(sourceLabel2)) {
							sourceLabel = sourceLabel1;
						}

						if (sourceLabel.trim().toLowerCase().contains("orcid")) {
							System.err.println("Children ID: " + orgCC.getId() + " is not only trusted by ORCID!!! ");
							continue;
						}

					} else if (sources.size() == 1) {

						Source s = sources.get(0);
						String sourceLabel = s.getLabel();

						if (sourceLabel.trim().toLowerCase().contains("orcid")) {
							System.err.println("Children ID: " + orgCC.getId() + " is not only trusted by ORCID!!! ");
							continue;
						}

					} else {
						throw new RuntimeException("Sources cannot be empty");
					}

				}

			}

			if (!entitymanager.contains(orgCC)) {
				System.err.println("Children ID: " + orgCC.getId() + " is not persisted!!! ");
				continue;
				// throw new RuntimeException("Children is not persisted!!!");
			}

			// HERE
			// orgCC.getActivities().clear();
			// orgCC.getBadges().clear();
			//
			// if (entitymanager.contains(orgCC)) {
			// System.err.println("\tORGANIZATION: " + idC + "\tLABEL: " + orgCC.getLabel()
			// + " already persisted");
			// } else {
			// System.err.println("\tORGANIZATION: " + idC + "\tLABEL: " + orgCC.getLabel()
			// + " NOT PERSISTED!!!");
			// }
			// List<Source> sou = new ArrayList<>();
			// for (Source idS : orgCC.getSources()) {
			// if (!entitymanager.contains(idS)) {
			// System.out.println("\tSources: " + idS.getLabel() + " NOT PERSISTED!");
			// } else {
			// System.out.println("\tSources: " + idS.getLabel() + " already persisted");
			// }
			// sou.add(idSources.get(idS.getId()));
			// }
			// orgCC.setSources(sou);
			// // filter
			// Set<Publication> pubs = new HashSet<>();
			// pubs.addAll(orgCC.getPublications());
			// List<Publication> pubsList = new ArrayList<>();
			// pubsList.addAll(pubs);
			//
			// orgCC.setPublications(pubsList);
			//
			// Set<Person> people = new HashSet<>();
			// people.addAll(orgCC.getPeople());
			//
			// List<Person> peopleList = new ArrayList<>();
			// peopleList.addAll(people);
			// orgCC.setPeople(resolvePeople(peopleList));

			List<Person> peopleChildren = orgCC.getPeople();
			for (Person p : peopleChildren) {
				if (peopleOrg.contains(p)) {
					System.err.println("Same person is persisted in parent and children structure!!!");
				}
			}

			orgs.add(orgCC);

		}

		org.setChildrenOrganizations(orgs);
		// HERE
		// org.getActivities().clear();
		// org.getBadges().clear();
		//
		// Set<Publication> pubs = new HashSet<>();
		// pubs.addAll(org.getPublications());
		// List<Publication> pubsList = new ArrayList<>();
		// pubsList.addAll(pubs);
		//
		// Set<Person> people = new HashSet<>();
		// people.addAll(org.getPeople());
		//
		// List<Person> peopleList = new ArrayList<>();
		// peopleList.addAll(people);
		//
		// org.setPeople(resolvePeople(peopleList));
		//
		// org.setPublications(pubsList);

		// List<SpinoffFrom> spins = new ArrayList<>();
		// spins.addAll(org.getSpinoffFroms());
		// for (SpinoffFrom sp : spins) {
		// sp.setOrganization(org);
		// }
		// org.setSpinoffFroms(spins);
		//
		// List<OrganizationExtraField> organizationExtraFieldsList = new ArrayList<>();
		// organizationExtraFieldsList.addAll(org.getOrganizationExtraFields());
		// for (OrganizationExtraField orgEF : organizationExtraFieldsList) {
		// orgEF.setOrganization(org);
		// }
		//
		// org.setOrganizationExtraFields(organizationExtraFieldsList);
		//
		// List<OrganizationIdentifier> organizationIdentifiersList = new ArrayList<>();
		// organizationIdentifiersList.addAll(org.getOrganizationIdentifiers());
		//
		// for (OrganizationIdentifier oid : organizationIdentifiersList) {
		// oid.setOrganization(org);
		// }
		//
		// org.setOrganizationIdentifiers(organizationIdentifiersList);
		//
		// List<OrganizationRelation> organizationRelationsList = new ArrayList<>();
		// organizationRelationsList.addAll(org.getOrganizationRelations());
		// for (OrganizationRelation orgR : organizationRelationsList) {
		// orgR.setOrganization(org);
		// }
		//
		// org.setOrganizationRelations(organizationRelationsList);

		if (org.getChildrenOrganizations().size() > 0) {
			// entitymanager.merge(org);
		}
	}

	private static List<Person> resolvePeople(List<Person> people) {

		Map<String, List<Person>> nameSurnamePeople = new HashMap<>();
		for (Person p : people) {
			String nameSurname = p.getFirstName() + " " + p.getLastName();

			nameSurname = nameSurname.toLowerCase();

			if (nameSurnamePeople.containsKey(nameSurname)) {
				nameSurnamePeople.get(nameSurname).add(p);
			} else {
				List<Person> pp = new ArrayList<>();
				pp.add(p);
				nameSurnamePeople.put(nameSurname, pp);
			}

		}

		List<Person> peopleCleaned = new ArrayList<>();

		for (Map.Entry<String, List<Person>> entry : nameSurnamePeople.entrySet()) {

			Set<Source> sourcesSet = new HashSet<>();
			Person per = entry.getValue().get(0);

			String title = "";
			String email = "";

			for (Person p : entry.getValue()) {
				// integrate here
				sourcesSet.addAll(p.getSources());

				if (p.getEmail() != null) {
					if (!p.getEmail().trim().equals("")) {
						if (!email.equals("")) {
							email = p.getEmail();
						}
					}
				}
				if (p.getTitle() != null) {
					if (!p.getTitle().trim().equals("")) {
						if (!title.equals("")) {
							title = p.getTitle();
						}
					}
				}

			}

			List<Source> sources = new ArrayList<>();
			sources.addAll(sourcesSet);

			per.setSources(sources);
			per.setEmail(email);
			per.setTitle(title);
			peopleCleaned.add(per);
		}

		return peopleCleaned;

	}

	private static void insertOrganization(EntityManager entitymanager, Organization org, Integer ID,
			Map<Integer, Organization> idOrganization) {

		org.getChildrenOrganizations().clear();

		for (Organization orgC : org.getChildrenOrganizations()) {
			System.out.println("\tChildren\t" + orgC.getId() + "\t" + orgC.getLabel());
			// insertOrganization(entitymanager, orgC, ID);
		}

		// CHANGE HERE

		System.out.println(
				"\n\n\n=========================================================================================================================================================");
		// entitymanager.getTransaction().begin();
		System.out.println("ID: " + ID);
		System.out.println("LABEL: " + org.getLabel());
		// System.out.println("Label: " + org.getLabel());
		System.out.println("SOURCE:");
		for (Source s : org.getSources()) {
			// entitymanager.persist(s);
			System.out.println("\t\t" + s.getLabel());
		}
		System.out.println("\nACTIVITY LIST:");
		for (OrganizationActivity act : org.getActivities()) {

			System.out.println("\t\t" + act.toString());
		}

		org.getActivities().clear();

		System.out.println("\nPROJECT:");
		List<String> pjs = new ArrayList<>();
		Set<Integer> projectIds = new HashSet<>();
		boolean nullCase = false;

		System.out.println("PROJECT INSERTION PHASE: ");

		for (Project proj : org.getProjects()) {

			System.out.println("PROJECT ID: " + proj.getId());
			if (!entitymanager.contains(proj)) {
				throw new RuntimeException("PROJECT: " + proj.getId() + " IS NOT PERSISTED!!!");
			}

		}

		entitymanager.persist(org);
		if (!entitymanager.contains(org)) {
			System.err.println("ENTITY: " + ID + "\t NOT PERSISTED + " + org.getLabel() + " org_id: " + org.getId());
		}
		idOrganization.put(ID, org);

	}

	// private static Organization createOrganizationDeepCopy(Organization
	// organization) {
	// Organization org = new Organization();
	// org.setLabel(organization.getLabel());
	//
	// org.setAcronyms(organization.getAcronyms());
	// org.setActivities(organization.getActivities());
	// org.setAddress(organization.getAddress());
	// org.setAddressSources(organization.getAddressSources());
	//
	// org.setAlias(organization.getAlias());
	// org.setBadges(organization.getBadges());
	// org.setChildrenOrganizations(organization.getChildrenOrganizations());
	//
	// org.setCity(organization.getCity());
	// org.setCityCode(organization.getCityCode());
	// org.setCommercialLabel(organization.getCommercialLabel());
	// org.setCountry(organization.getCountry());
	//
	// org.setCountryCode(organization.getCityCode());
	// org.setCreationYear(organization.getCreationYear());
	//
	// org.setFinancePrivateDate(organization.getFinancePrivateDate());
	// org.setFinancePrivateEmployees(organization.getFinancePrivateEmployees());
	// org.setFinancePrivateRevenueRange(organization.getFinancePrivateRevenueRange());
	//
	// org.setIsPublic(organization.getIsPublic());
	// org.setLat(organization.getLat());
	// org.setLeaders(organization.getLeaders());
	// org.setLinks(organization.getLinks());
	//
	// org.setLon(organization.getLon());
	//
	// org.setOrganizationExtraFields(organization.getOrganizationExtraFields());
	// org.setOrganizationIdentifiers(organization.getOrganizationIdentifiers());
	// org.setOrganizationRelations(organization.getOrganizationRelations());
	//
	// org.setOrganizationType(organization.getOrganizationType());
	// org.setPeople(organization.getPeople());
	//
	// org.setPostcode(organization.getPostcode());
	// org.setProjects(organization.getProjects());
	//
	// org.setPublications(organization.getPublications());
	// org.setSources(organization.getSources());
	// org.setSpinoffFroms(organization.getSpinoffFroms());
	// org.setTypeCategoryCode(organization.getTypeCategoryCode());
	// org.setTypeKind(organization.getTypeKind());
	// org.setTypeLabel(organization.getTypeLabel());
	// org.setUrbanUnit(organization.getUrbanUnit());
	// org.setUrbanUnitCode(organization.getUrbanUnitCode());
	//
	// return org;
	//
	// }

	private Map<Integer, Integer> resolveOrganizations(Set<Set<String>> organizationCorrespondances,
			Map<Integer, Organization> idOrganization) {
		Map<Integer, Integer> organizationResolutionMap = new HashMap<>();

		Map<String, Integer> sourceLabelValue = new HashMap<>();

		sourceLabelValue.put("OpenAire", 3);
		sourceLabelValue.put("ScanR", 2);
		sourceLabelValue.put("Arianna - Anagrafe Nazionale delle Ricerche", 1);
		sourceLabelValue.put("Questio", 5);
		sourceLabelValue.put("Startup - Registro delle imprese", 1);
		sourceLabelValue.put("ORCID", 5);
		sourceLabelValue.put("Consiglio Nazionale delle Ricerche (CNR)", 1);
		sourceLabelValue.put("P3", 1);
		sourceLabelValue.put("Grid", 3);
		sourceLabelValue.put("Patiris", 3);
		sourceLabelValue.put("Crawled", 4);
		sourceLabelValue.put("Austrian Science Fund (FWF)", 1);
		sourceLabelValue.put("CercaUniversita", 1);
		sourceLabelValue.put("SICRIS - Slovenian Current Research Information System", 1);
		for (Set<String> component : organizationCorrespondances) {
			Integer candidateRight = null;
			List<Integer> comp = new ArrayList<>();
			for (String co : component) {
				comp.add(Integer.parseInt(co));
			}

			Collections.sort(comp, new Comparator<Integer>() {

				@Override
				public int compare(Integer o1, Integer o2) {

					Organization p1 = idOrganization.get(o1);

					Organization p2 = idOrganization.get(o2);

					Source s1 = p1.getSources().get(0);
					Source s2 = p2.getSources().get(0);

					String label1 = s1.getLabel();
					String label2 = s2.getLabel();

					Integer score1 = sourceLabelValue.get(label1);
					Integer score2 = sourceLabelValue.get(label2);

					if (score1 == null) {
						score1 = Integer.MAX_VALUE;
						System.err.println("Source " + label1 + " is not classified");
					}

					if (score2 == null) {
						score2 = Integer.MAX_VALUE;
						System.err.println("Source " + label2 + " is not classified");
					}

					return Integer.compare(score1, score2);
				}
			});

			candidateRight = comp.get(0);
			organizationResolutionMap.put(candidateRight, candidateRight);
			for (int i = 1; i < comp.size(); i++) {
				organizationResolutionMap.put(comp.get(i), candidateRight);
			}

		}

		return organizationResolutionMap;
	}

	private Map<Integer, Integer> resolveProjects(Set<String> project, Set<Set<String>> projectCorrespondances,
			Map<Integer, Project> idProject) {
		Map<Integer, Integer> projectResolutionMap = new HashMap<>();

		Map<String, Integer> sourceLabelValue = new HashMap<>();

		sourceLabelValue.put("OpenAire", 1);
		sourceLabelValue.put("ScanR", 2);
		sourceLabelValue.put("Arianna - Anagrafe Nazionale delle Ricerche", 3);
		sourceLabelValue.put("Questio", 4);
		sourceLabelValue.put("P3", 1);
		sourceLabelValue.put("Austrian Science Fund (FWF)", 1);
		sourceLabelValue.put("SICRIS - Slovenian Current Research Information System", 1);

		for (Set<String> component : projectCorrespondances) {
			Integer candidateRight = null;
			List<Integer> comp = new ArrayList<>();
			// try {
			for (String co : component) {
				comp.add(Integer.parseInt(co));
			}

			Collections.sort(comp, new Comparator<Integer>() {

				@Override
				public int compare(Integer o1, Integer o2) {

					Project p1 = idProject.get(o1);

					Project p2 = idProject.get(o2);

					Source s1 = p1.getSources().get(0);
					Source s2 = p2.getSources().get(0);

					String label1 = s1.getLabel();
					String label2 = s2.getLabel();

					Integer score1 = sourceLabelValue.get(label1);
					Integer score2 = sourceLabelValue.get(label2);

					if (score1 == null) {
						score1 = Integer.MAX_VALUE;
						System.err.println("Source " + label1 + " is not classified");
					}

					if (score2 == null) {
						score2 = Integer.MAX_VALUE;
						System.err.println("Source " + label2 + " is not classified");
					}

					return Integer.compare(score1, score2);
				}
			});

			Set<Source> sources = new HashSet<>();
			for (Integer id : comp) {
				sources.addAll(idProject.get(id).getSources());
			}

			List<Source> sourceUpdated = new ArrayList<>();
			sourceUpdated.addAll(sources);

			candidateRight = comp.get(0);
			projectResolutionMap.put(candidateRight, candidateRight);
			idProject.get(candidateRight).setSources(sourceUpdated);
			for (int i = 1; i < comp.size(); i++) {
				projectResolutionMap.put(comp.get(i), candidateRight);
			}
			// } catch (Exception e) {
			// System.err.println("Wrong label!!! " + e.getMessage());
			// System.err.println("Component: " + component.toString());
			// }

		}

		return projectResolutionMap;
	}

	private Map<String, Project> retriveProjectMap(List<Organization> organizations) {

		Map<String, Project> idProject = new HashMap<>();

		for (Organization o : organizations) {
			for (Project p : o.getProjects()) {
				if (!idProject.containsKey(p.getId() + "")) {
					String id = p.getId() + "";
					idProject.put(id, p);
				}
			}
		}
		return idProject;
	}

	private Set<String> retrieveAllProjectIds(List<Organization> organizations) {
		Set<String> ids = new HashSet<>();
		for (Organization o : organizations) {
			for (Project p : o.getProjects()) {
				String id = p.getId() + "";
				ids.add(id);
			}
		}
		return ids;
	}

	public static Object objExists(EntityManager em, Class entity, String attValue) {

		Object res = em.find(entity, attValue);

		return res;

	}

	public static Object objExists(EntityManager em, Class entity, Integer attValue) {

		Object res = em.find(entity, attValue);

		return res;

	}

	private Organization compressDuplicatesOrganizations(List<Organization> orgs,
			Map<Integer, Integer> projectResolutionMap, Map<String, Project> idProjects) {

		Organization cand = orgs.get(0);
		Set<Integer> childrenOrganizationIds = new HashSet<>();
		Set<Integer> badgeIds = new HashSet<>();
		Set<Integer> leaderIds = new HashSet<>();
		Set<Integer> peopleIds = new HashSet<>();
		Set<Integer> publicationIds = new HashSet<>();
		Set<String> activityCodes = new HashSet<>();

		Set<Integer> linksIds = new HashSet<>();
		Set<Integer> projectsIds = new HashSet<>();

		Set<OrganizationActivity> listMerda = new HashSet<>();
		listMerda.addAll(cand.getActivities());

		System.out.println("CAND: " + cand.getLabel());
		for (Source s : cand.getSources()) {
			System.out.println("\t" + s.getLabel());
		}
		for (int i = 1; i < orgs.size(); i++) {

			Organization item = orgs.get(i);
			cand.getOrganizationExtraFields().addAll(item.getOrganizationExtraFields());
			cand.getOrganizationIdentifiers().addAll(item.getOrganizationIdentifiers());
			cand.getOrganizationRelations().addAll(item.getOrganizationRelations());

			// cand.getChildrenOrganizations().addAll(item.getChildrenOrganizations());

			for (Organization org : cand.getChildrenOrganizations()) {
				childrenOrganizationIds.add(org.getId());
			}

			for (Organization child : cand.getChildrenOrganizations()) {
				if (!childrenOrganizationIds.contains(child.getId())) {
					childrenOrganizationIds.add(child.getId());
					cand.getChildrenOrganizations().add(child);
				} else {
					// add children duplicate!!!
				}
			}

			for (OrganizationActivity oa : cand.getActivities()) {
				activityCodes.add(oa.getCode());

			}

			for (OrganizationActivity act : item.getActivities()) {

				if (!activityCodes.contains(act.getCode())) {
					activityCodes.add(act.getCode());
					cand.getActivities().add(act);
				}
			}

			// only for debug
			listMerda.addAll(item.getActivities());

			// cand.getBadges().addAll(item.getBadges());

			for (Badge badge : cand.getBadges()) {
				badgeIds.add(badge.getId());
			}

			for (Badge b : item.getBadges()) {
				if (!badgeIds.contains(b.getId())) {
					badgeIds.add(b.getId());
					cand.getBadges().add(b);
				}
			}

			// cand.getLeaders().addAll(item.getLeaders());

			for (Leader l : cand.getLeaders()) {
				leaderIds.add(l.getId());
			}

			for (Leader l : item.getLeaders()) {
				if (!leaderIds.contains(l.getId())) {
					leaderIds.add(l.getId());
					cand.getLeaders().add(l);
				}
			}

			for (Person p : cand.getPeople()) {
				peopleIds.add(p.getId());
			}

			for (Person p : item.getPeople()) {
				if (!peopleIds.contains(p.getId())) {
					peopleIds.add(p.getId());
					cand.getPeople().add(p);
				}
			}

			for (Publication p : cand.getPublications()) {
				publicationIds.add(p.getId());
			}

			for (Publication p : item.getPublications()) {
				if (!publicationIds.contains(p.getId())) {
					publicationIds.add(p.getId());
					cand.getPublications().add(p);
				}
			}

			// cand.getLinks().addAll(item.getLinks());

			for (Link link : cand.getLinks()) {
				linksIds.add(link.getId());
			}

			for (Link link : item.getLinks()) {

				if (!linksIds.contains(link.getId())) {
					linksIds.add(link.getId());
					if (link.getUrl().contains("http://")) {
						cand.getLinks().add(link);
					} else {
						String url = link.getUrl();

						url = "http://" + url;
						link.setUrl(url);
						cand.getLinks().add(link);
					}
				}

			}

			// cand.getProjects().addAll(item.getProjects());
			if (cand.getProjects() != null) {
				for (Project project : cand.getProjects()) {
					if (project != null) {
						String id = project.getId() + "";
						projectsIds.add(projectResolutionMap.get(id));
					}
				}
			}

			if (item.getProjects() != null) {
				for (Project project : item.getProjects()) {
					if (!projectsIds.contains(projectResolutionMap.get(project.getId() + ""))) {
						Integer id = projectResolutionMap.get(project.getId());
						projectsIds.add(id);
						Project resolvedProject = idProjects.get(id);
						cand.getProjects().add(resolvedProject);
					}

				}
			}

			for (Source s : item.getSources()) {
				if (!cand.getSources().contains(s)) {
					cand.getSources().add(s);
				}
			}

			if (cand.getAcronyms() == null) {
				if (item.getAcronyms().size() > 0) {

					for (String acronym : item.getAcronyms()) {

						if (!acronym.equals("") && !acronym.toLowerCase().equals("null")) {
							if (!cand.getAcronyms().contains(acronym)) {
								cand.getAcronyms().add(acronym);
							}
						}
					}

				}

			} else {
				for (String acronym : item.getAcronyms()) {
					if (acronym.equals("") || acronym.toLowerCase().equals("null")) {
						if (item.getAcronyms().size() > 0) {
							if (!acronym.equals("") && !acronym.toLowerCase().equals("null")) {
								if (!cand.getAcronyms().contains(acronym)) {
									cand.getAcronyms().add(acronym);
								}
							}
						}
					}
				}
			}

			// cand.setAcronyms();

			// cand.getAddress();

			if (cand.getAddress() == null) {
				if (item.getAddress() != null) {
					if (!item.getAddress().equals("") && !item.getAddress().toLowerCase().equals("null")) {
						cand.setAddress(item.getAddress());
					}
				}

			} else if (cand.getAddress().equals("") || cand.getAddress().toLowerCase().equals("null")) {
				if (item.getAddress() != null) {
					if (!item.getAddress().equals("") && !item.getAddress().toLowerCase().equals("null")) {
						cand.setAddress(item.getAddress());
					}
				}
			}

			// cand.getCity();

			if (cand.getCity() == null) {
				if (item.getCity() != null) {
					if (!item.getCity().equals("") && !item.getCity().toLowerCase().equals("null")) {
						cand.setCity(item.getCity());
					}
				}

			} else if (cand.getCity().equals("") || cand.getCity().toLowerCase().equals("null")) {
				if (item.getCity() != null) {
					if (!item.getCity().equals("") && !item.getCity().toLowerCase().equals("null")) {
						cand.setCity(item.getCity());
					}
				}
			}

			// cand.getPostcode();
			if (cand.getPostcode() == null) {
				if (item.getPostcode() != null) {
					if (!item.getPostcode().equals("") && !item.getPostcode().toLowerCase().equals("null")) {
						cand.setPostcode(item.getPostcode());
					}
				}

			} else if (cand.getPostcode().equals("") || cand.getPostcode().toLowerCase().equals("null")) {
				if (item.getPostcode() != null) {
					if (!item.getPostcode().equals("") && !item.getPostcode().toLowerCase().equals("null")) {
						cand.setPostcode(item.getPostcode());
					}
				}
			}

		}

		// Set<OrganizationActivity> activitiesSet = new HashSet<>();
		// activitiesSet.addAll(cand.getActivities());
		// if (activitiesSet.size() != cand.getActivities().size()) {
		//
		// System.out.println("Number of activities LIST: " +
		// cand.getActivities().size());
		// System.out.println("Number of activities SET: " + activitiesSet.size());
		// }

		if (listMerda.size() > 0) {
			System.out.println("--------------------------------------");
			for (OrganizationActivity act : listMerda) {
				System.out.println("\t\t\t" + act.toString());
			}
			System.out.println("--------------------------------------");
		}

		if (cand.getActivities().size() > 0) {
			System.out.println("LABEL: " + cand.getLabel());

			System.out.println("LIST OF ODACTS: ");
			for (OrganizationActivity act : cand.getActivities()) {
				System.out.println("\t\t\t" + act.toString());
			}
		}
		return cand;
	}

	public DeduplicatorUtilTest() {
		// TODO Auto-generated constructor stub
	}

	public Map<String, Organization> getOrganizationMap(List<Organization> organizations) {
		Map<String, Organization> orgMap = new HashMap<>();
		for (Organization org : organizations) {
			orgMap.put(org.getId() + "", org);
		}
		return orgMap;
	}

	public Set<String> retrieveAllOrganizationIds(List<Organization> organizations) {
		Set<String> ids = new HashSet<>();
		for (Organization org : organizations) {
			ids.add(org.getId() + "");
		}
		return ids;
	}

	public List<Organization> retrieveOrganizations(String sourceConnection, boolean includeORCID) {
		List<Organization> organizationsAll = new ArrayList<>();

		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(sourceConnection);
		EntityManager entitymanager = emfactory.createEntityManager();

		entitymanager.getTransaction().begin();

		String sourceRevisionDate = "2017-07-01";
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

		Date sourceDate = null;
		try {
			sourceDate = df.parse(sourceRevisionDate);
			// source.setRevisionDate(sourceDate);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Date revisionDate = new Date(2017, 12, 15);
		String sourceLabel = "ORCID";

		Source s = null;

		Query query1 = entitymanager
				.createQuery("SELECT s FROM Source s WHERE s.label = :label and s.revisionDate = :revision_date");
		query1.setParameter("label", sourceLabel);
		query1.setParameter("revision_date", sourceDate);

		// Source s = new Source();
		// s.setLabel(sourceLabel);
		// s.setRevisionDate(revisionDate);
		// s.setUrl(url);

		List<Source> results = query1.getResultList();
		// System.out.println("******************");
		for (Source source : results) {
			System.out.println(source.getLabel());
		}
		// System.out.println("******************");
		if (results.size() > 0) {
			if (results.get(0) != null) {
				s = results.get(0);
				// System.out.println("ESISTE SOURCE");
			}
		}

		// exclude ORCID
		Query query = null;
		if (!includeORCID) {
			query = entitymanager.createQuery("Select o FROM Organization o where :source NOT MEMBER OF o.sources");
			query.setParameter("source", s);
		} else {
			query = entitymanager.createQuery("Select o FROM Organization o");
		}
		List<Organization> organizations = query.getResultList();

		organizationsAll.addAll(organizations);

		System.out.println("Number of organizations from database 1: " + organizations.size());

		entitymanager.close();
		emfactory.close();
		System.out.println("Number of all organizations: " + organizationsAll.size());
		return organizationsAll;
	}

	public List<Project> retrieveProjects(String sourceDB) {
		List<Project> allProject = new ArrayList<>();
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(sourceDB);
		EntityManager entitymanager = emfactory.createEntityManager();
		entitymanager.getTransaction().begin();
		Query query = entitymanager.createQuery("Select p FROM Project p");
		List<Project> projects = query.getResultList();
		allProject.addAll(projects);
		System.out.println("Number of all projects: " + allProject.size());
		entitymanager.close();
		emfactory.close();
		return allProject;
	}

	public List<OrganizationActivity> retrieveOrganizationActivity(String sourceDB) {
		List<OrganizationActivity> organizationActivity = new ArrayList<>();
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(sourceDB);
		EntityManager entitymanager = emfactory.createEntityManager();
		entitymanager.getTransaction().begin();
		Query query = entitymanager.createQuery("Select oa FROM OrganizationActivity oa");
		List<OrganizationActivity> organizationActivities = query.getResultList();
		organizationActivity.addAll(organizationActivities);
		System.out.println("Number of all organizationActivity: " + organizationActivity.size());
		entitymanager.close();
		emfactory.close();
		return organizationActivities;
	}

	public List<Source> retrieveSources(String sourceDB) {
		List<Source> sources = new ArrayList<>();
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(sourceDB);
		EntityManager entitymanager = emfactory.createEntityManager();
		entitymanager.getTransaction().begin();
		Query query = entitymanager.createQuery("Select s FROM Source s");
		List<Source> s = query.getResultList();
		sources.addAll(s);
		System.out.println("Number of all sources: " + sources.size());
		entitymanager.close();
		emfactory.close();
		return sources;
	}

	public Map<Integer, Organization> getMapOrganization(List<Organization> organizations) {
		Map<Integer, Organization> idOrganization = new HashMap<>();

		for (Organization org : organizations) {

			if (idOrganization.containsKey(org.getId())) {
				System.err.println("Duplicate entry!!! ");
				System.exit(-1);
			} else {
				idOrganization.put(org.getId(), org);
			}
		}

		return idOrganization;
	}

	public Map<Integer, Project> getMapProject(List<Project> projects) {
		Map<Integer, Project> idProject = new HashMap<>();
		for (Project prj : projects) {
			if (idProject.containsKey(prj.getId())) {
				System.err.println("Duplicate entry!!! ");
				System.exit(-1);
			} else {
				idProject.put(prj.getId(), prj);
			}
		}
		return idProject;
	}

	public Map<Integer, Source> getMapSource(List<Source> sources) {
		Map<Integer, Source> idSources = new HashMap<>();
		for (Source src : sources) {

			if (idSources.containsKey(src.getId())) {
				System.err.println("Duplicate entry!!! ");
				System.exit(-1);
			} else {
				idSources.put(src.getId(), src);
			}
		}
		return idSources;
	}

	private Map<Integer, Person> getMapPeople(List<Person> retrievePeople) {
		Map<Integer, Person> idPeople = new HashMap<>();

		for (Person p : retrievePeople) {
			if (idPeople.containsKey(p.getId())) {
				System.err.println("Duplicate entry!!! ");
				System.exit(-1);
			} else {
				idPeople.put(p.getId(), p);
			}
		}
		return idPeople;
	}

	private List<Person> retrievePeople(String sourceDB) {
		List<Person> people = new ArrayList<>();
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(sourceDB);
		EntityManager entitymanager = emfactory.createEntityManager();
		entitymanager.getTransaction().begin();
		Query query = entitymanager.createQuery("Select s FROM Person s");
		List<Person> s = query.getResultList();
		people.addAll(s);
		System.out.println("Number of all sources: " + people.size());
		entitymanager.close();
		emfactory.close();

		return people;
	}

	private Map<Integer, Publication> getMapPublications(List<Publication> retrievePublications) {
		Map<Integer, Publication> idPublication = new HashMap<>();

		for (Publication p : retrievePublications) {
			if (idPublication.containsKey(p.getId())) {
				System.err.println("Duplicate entry!!! ");
				System.exit(-1);
			} else {
				idPublication.put(p.getId(), p);
			}
		}
		return idPublication;
	}

	private List<Publication> retrievePublications(String sourceDB) {
		List<Publication> publication = new ArrayList<>();
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory(sourceDB);
		EntityManager entitymanager = emfactory.createEntityManager();
		entitymanager.getTransaction().begin();
		Query query = entitymanager.createQuery("Select s FROM Publication s");
		List<Publication> s = query.getResultList();
		publication.addAll(s);
		System.out.println("Number of all publications: " + publication.size());
		entitymanager.close();
		emfactory.close();
		return publication;
	}

	public Map<String, OrganizationActivity> getMapOrganizationActivity(
			List<OrganizationActivity> organizationActivities) {
		Map<String, OrganizationActivity> idOrganizationActivity = new HashMap<>();
		for (OrganizationActivity act : organizationActivities) {
			if (idOrganizationActivity.containsKey(act.getCode())) {
				System.err.println("Duplicate entry!!! ");
				System.exit(-1);
			} else {
				idOrganizationActivity.put(act.getCode(), act);
			}
		}

		return idOrganizationActivity;
	}

	public Set<Set<String>> readCorrespondences(String path) {

		Set<Set<String>> correspondes = new HashSet<>();

		File fileDir = new File(path);

		try {

			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(fileDir), "UTF8"));

			String str;
			int lineCount = 0;
			while ((str = in.readLine()) != null) {

				if (!str.trim().equals("")) {
					String[] items = str.split("\t");
					Set<String> corr = new HashSet<>();
					for (String item : items) {
						corr.add(item);
					}
					if (correspondes.contains(corr)) {
						System.out.println("Duplicate: " + corr.toString());
					}
					if (corr.size() != 1) {

						correspondes.add(corr);
					}

				}
				lineCount++;
				// System.out.println(str);
			}

			in.close();
			System.out.println("Number of lines: " + lineCount);
		} catch (UnsupportedEncodingException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println(e.getMessage());
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}

		return correspondes;

	}

	public Set<Set<String>> readCorrespondencesManual(String path, Set<Set<String>> correspondence,
			Map<String, Integer> LID_ID) {

		Set<Set<String>> correspondes = new HashSet<>();

		File fileDir = new File(path);

		try {

			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(fileDir), "UTF8"));

			String str;
			int lineCount = 0;
			while ((str = in.readLine()) != null) {

				if (!str.trim().equals("")) {
					String[] items = str.split("\t");
					Set<String> corr = new HashSet<>();
					for (String item : items) {

						if (LID_ID.containsKey(item)) {
							corr.add(LID_ID.get(item) + "");
						} else {
							throw new RuntimeException("LID is not present!!!");
						}

					}
					if (correspondes.contains(corr)) {
						System.out.println("Duplicate: " + corr.toString());
					}
					if (corr.size() != 1) {

						correspondes.add(corr);
					}

				}
				lineCount++;
				// System.out.println(str);
			}

			in.close();
			System.out.println("Number of lines: " + lineCount);
		} catch (UnsupportedEncodingException e) {
			System.out.println(e.getMessage());
		} catch (IOException e) {
			System.out.println(e.getMessage());
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}

		return correspondes;

	}

}
