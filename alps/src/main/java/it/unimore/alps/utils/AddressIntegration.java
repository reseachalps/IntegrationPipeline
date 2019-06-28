package it.unimore.alps.utils;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import it.unimore.alps.sql.model.Badge;
import it.unimore.alps.sql.model.Leader;
import it.unimore.alps.sql.model.Link;
import it.unimore.alps.sql.model.Organization;
import it.unimore.alps.sql.model.OrganizationActivity;
import it.unimore.alps.sql.model.OrganizationIdentifier;
import it.unimore.alps.sql.model.OrganizationRelation;
import it.unimore.alps.sql.model.OrganizationType;
import it.unimore.alps.sql.model.Source;
import it.unimore.alps.sql.model.SpinoffFrom;

public class AddressIntegration {
	
	private Reader reader;

	public static void main(String[] args) {
		
		CommandLine commandLine;
        Option addressFileOption = Option.builder("addressFile")
        		.hasArg()
        		.required(true)
        		.desc("The file that contains organization addresses data. ")
        		.longOpt("addressFile")
        		.build();

        Options options = new Options();
        CommandLineParser parser = new DefaultParser();

        options.addOption(addressFileOption);
        
        String addressFile = null;
                	
        try {
			commandLine = parser.parse(options, args);
		
			System.out.println("----------------------------");
			System.out.println("OPTIONS:");
			
	        if (commandLine.hasOption("addressFile")) {   
	        	addressFile = commandLine.getOptionValue("addressFile");
	        	System.out.println("\tOrganization addresses data file: " + addressFile);
			} else {
				System.out.println("\tOrganization addresses data file not provided. Use the addressFile option.");
	        	System.exit(1);
			}
			
			System.out.println("----------------------------\n");
        
        } catch (org.apache.commons.cli.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
        
        // -addressFile /home/matteo/Scrivania/coreex-new-addresses.json
        
        System.out.println("Starting address integration...");
        AddressIntegration addressIntegrator = new AddressIntegration(addressFile);
        addressIntegrator.integrateData();
        System.out.println("Address integration terminated.");

	}
	
	public AddressIntegration(String addressFile) {
		
		try {
			this.reader = new FileReader(addressFile);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private List<Organization> searchOrganizationByLink(EntityManager entitymanager, String url, boolean exact) {
		
		List<Organization> matchedOrganizations = new ArrayList<>();
		
		Query queryLink = null;
		if (exact == true) {
			queryLink = entitymanager.createQuery("SELECT l FROM Link l WHERE l.url=:url");
			queryLink.setParameter("url", url);
		} else {
			if (!url.equals("www")) {
				queryLink = entitymanager.createQuery("SELECT l FROM Link l WHERE l.url LIKE \"%" + url + "%\"");
			}
		}
		
		if (queryLink != null) {
			List<Link> links = queryLink.getResultList();
			if(links != null && links.size() > 0) {
				//System.out.println("Found links");
				for(Link link: links) {
					
					Query queryOrg = entitymanager.createQuery("Select o FROM Organization o WHERE :id MEMBER OF o.links");
					queryOrg.setParameter("id", link);
					List<Organization> orgs = queryOrg.getResultList();
					
					if (orgs != null && orgs.size() > 0) {
						//System.out.println("Found organizations");
						for(Organization org: orgs) {
							matchedOrganizations.add(org);
							//System.out.println("\t" + org.getAddress() + " " + org.getPostcode() + " " + org.getCity());
						}
					}
					
				}
				
			}
		}
			
		return matchedOrganizations;
		
	}
	
	public void integrateAddresses(EntityManager entitymanager) {
		
		JSONParser parser = new JSONParser();
		int count = 0;
		int updatedOrg = 0;
		
		try {
			
			Object fileObj = parser.parse(reader);
			JSONArray addressesArray = (JSONArray) fileObj;
			Iterator<JSONObject> it = addressesArray.iterator();
			while (it.hasNext()) {
				if((count%100) == 0) {
					//System.out.println("Updated " + updatedOrg + " organizations.");
					System.out.println(count);
				}
				count ++;
				entitymanager.getTransaction().begin();
				
				JSONObject addressData = it.next();
				
				List<Organization> matchedOrganizations = null;
				
				String websiteId = (String) addressData.get("website_id");
				if (websiteId != null) {
					matchedOrganizations = searchOrganizationByLink(entitymanager, websiteId, false);
				}
								
				String url = (String) addressData.get("url");
				if (matchedOrganizations != null) {	
					// organizations not found by website id: I try to search the organization by url field
					if (matchedOrganizations.size() == 0) {
						
						if (url != null) {
							matchedOrganizations = searchOrganizationByLink(entitymanager, url, true);
						}
					}
				}
				
				// debug only
				/*if (matchedOrganizations != null) {	
					if (matchedOrganizations.size() == 0) {
						String debugMessage = "Organization not found: ";
						if (websiteId == null) {
							debugMessage+="websiteId=null, ";
						} else {
							debugMessage+="websiteId="+ websiteId + ", ";
						}
						if (url == null) {
							debugMessage+="url=null.";
						} else {
							debugMessage+="url="+ url + ".";
						}
						System.out.println(debugMessage);
					}
				}*/
				
				// select the first address data
				String selectedAddress = null;
				String selectedZipCode = null;
				String selectedCity = null;
				JSONArray addresses = (JSONArray) addressData.get("addresses");
				if(addresses != null) {
					Iterator<JSONObject> itAddress = addresses.iterator();
					while (itAddress.hasNext()) {
						
						JSONObject address = itAddress.next();
						
						selectedAddress = (String) address.get("address");
						selectedZipCode = (String) address.get("zipcode");
						selectedCity = (String) address.get("city");
						
						break;
					}
					
				}
					
				
				if (matchedOrganizations != null && matchedOrganizations.size() > 0) {
					for (Organization org: matchedOrganizations) {
						boolean update = false;
						if (org.getAddress() == null || org.getAddress().equals("")) {
							if (selectedAddress != null) {
								org.setAddress(selectedAddress);
								update = true;
							}
						}
						if (org.getPostcode() == null || org.getPostcode().equals("")) {
							if (selectedZipCode != null) {
								org.setPostcode(selectedZipCode);
								update = true;
							}
						}
						if (org.getCity() == null || org.getCity().equals("")) {
							if (selectedCity != null) {
								org.setCity(selectedCity);
								update = true;
							}
						}
						if (update == true) {
							//entitymanager.merge(org);
							updatedOrg+=1;
						}
					}
				}
			
	        	entitymanager.getTransaction().commit();
				
			}

			reader.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		
		System.out.println("Updated " + updatedOrg + " organizations.");
		
	}
	
	public void integrateData() {
		
		EntityManagerFactory emfactory = Persistence.createEntityManagerFactory("alpsDedup");
		EntityManager entitymanager = emfactory.createEntityManager();
		
    	integrateAddresses(entitymanager);
    	
    	entitymanager.close();
		emfactory.close();
		
	}
		
}
