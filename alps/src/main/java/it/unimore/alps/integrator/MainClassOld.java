package it.unimore.alps.integrator;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class MainClassOld {
	private static int conta=0;
	private static int startLine=0;		//linea iniziale da cui parto a fare richieste
	public static void main(String[] args) throws JSONException {
		
		Set<String> sources = new HashSet<String>(Arrays.asList("OpenAire", "Arianna - Anagrafe Nazionale delle Ricerche", "Bureau van Dijk", "CercaUniversita", "Consiglio Nazionale delle Ricerche (CNR)", "Patiris", "Questio", "Startup - Registro delle imprese"));
		
//        String csvFile = "/home/matteop/csv/organizations_OpenAire.csv";
//        String csvFile = "/home/matteop/csv/organizations_Arianna - Anagrafe Nazionale delle Ricerche.csv";
//        String csvFile = "/home/matteop/csv/organizations_Bureau van Dijk.csv";
//        String csvFile = "/home/matteop/csv/organizations_CercaUniversita.csv";
//        String csvFile = "/home/matteop/csv/organizations_Consiglio Nazionale delle Ricerche (CNR).csv";
//        String csvFile = "/home/matteop/csv/organizations_ORCID.csv";
//        String csvFile = "/home/matteop/csv/organizations_Patiris.csv";
//        String csvFile = "/home/matteop/csv/organizations_Questio.csv";
//        String csvFile = "/home/matteop/csv/organizations_ScanR.csv";
//        String csvFile = "/home/matteop/csv/organizations_Startup - Registro delle imprese.csv";
        //String outputFile="/home/matteop/organizations_OpenAire.csv";
		 
		for (String source: sources) {
			String csvFile = "/home/matteop/csv/organizations_" + source + ".csv";
			String outputFile = "/home/matteop/organizations_" + source + ".csv";
		
			conta=0;
			startLine=0;

	        BufferedReader br = null;
	        BufferedWriter bw = null;
	        String line = "";
	        
	        findStartLine(line,br,bw,outputFile);
	        try {
	            br = new BufferedReader(new FileReader(csvFile));
	        	bw = new BufferedWriter(new FileWriter(outputFile,true));
	            readCSV(line,br,bw,outputFile);
	        } catch (FileNotFoundException e) {
	            e.printStackTrace();
	        } catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
	            if (br != null) {
	                try {
	                    br.close();
	                } catch (IOException e) {
	                    e.printStackTrace();
	                }
	            }
	            if (bw != null) {
	                try {
	                    bw.close();
	                } catch (IOException e) {
	                    e.printStackTrace();
	                }
	            }
	        }
		}
	}
	//trovo riga iniziale da cui ricominciare a mandare richieste
	private static void findStartLine(String line,BufferedReader br,BufferedWriter bw,String outputFile) {
        try {
        	br = new BufferedReader(new FileReader(outputFile));
		} catch (FileNotFoundException e1) {
			startLine=0;
			return;
		}
        try {
			while ((line = br.readLine()) != null) {
				String[] tokens = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
				if (tokens.length<16) {
					break;
				}
				if(tokens[14]=="0.0" && tokens[15]=="0.0") {
					break;
				}else {
					startLine++;
				}				
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
        try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void readCSV(String line,BufferedReader br,BufferedWriter bw,String outputFile) {
		int i=0;
		String latlon[]= {"0.0","0.0"};
		boolean firstLine=true;
		String c="",a="";
		String outputLine="";
		PrintWriter out= new PrintWriter(bw);
        try {
			while ((line = br.readLine()) != null) {
			    String[] tokens = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
			    if(conta>=startLine) {
			    	if(startLine!=0) {
			    		firstLine=false;
			    	}
				    if(firstLine==false) {
					    System.out.println("\n---------------------------");
					    for(String t : tokens) {
					    	if(i==6) {
						   		System.out.println("> "+t);
						    	a=t;
						    	outputLine=outputLine+t+",";
					    	}else if(i==7) {
						   		System.out.println("> "+t);
					    		c=t;
					    		outputLine=outputLine+t+",";
							    try {
							    		latlon=request(c,a);
								    }catch(Exception e) {
								    	System.out.println("ERRORE");
								    }
					    	}else if(i==14) {
					    		outputLine=outputLine+latlon[0]+",";
					    	}else if(i==15) {
					    		outputLine=outputLine+latlon[1]+",";
					    	}else {
					    		outputLine=outputLine+t+",";
					    	}
					    	i++;
					    }
					    outputLine=outputLine.substring(0,outputLine.length()-1);
					    i=0;
				    }else {	//� la prima riga
				    	for(String t : tokens) {
					    	outputLine=outputLine+t+",";
				    	}
				    }
				    out.println(outputLine);
			    }
			    bw.flush();
			    conta++;
			    firstLine=false;
			    outputLine="";
			    latlon[0]= "0.0";
			    latlon[1]= "0.0";
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
        try {
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static String[] request(String city,String address) {
		 String ret[]= {"0.0","0.0"};
	     JSONObject Jobject;
	     JSONArray arrayLoc;
		 city=city.replaceAll(" ", "%20");
		 address=address.replaceAll(" ", "%20");	
		     OkHttpClient client = new OkHttpClient();

		     Request request = new Request.Builder()
		       .url("https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates?f=json&singleLine="+address+"%20"+city+"&outFields=Match_addr%2CAddr_type")
		       .get()
		       .addHeader("cache-control", "no-cache")
		       .addHeader("postman-token", "25c9935b-d60f-f19f-9134-8aa035af2181")
		       .build();
		     try {
		    	 Response response = client.newCall(request).execute();
			     String jsonData = response.body().string();
				try {
					Jobject = new JSONObject(jsonData);
					arrayLoc = Jobject.getJSONArray("candidates");
				    String Y = arrayLoc.toString();
				    String X = arrayLoc.toString();
				    Y=StringUtils.substringBetween(Y, "\"y\":", "},\"");
				    X=StringUtils.substringBetween(X, "\"x\":", ",\"y\"");
					ret[0]=Y;
					ret[1]=X;
					System.out.println("\n"+conta);
					System.out.println("\n"+Y+"-------"+X);
				} catch (JSONException e) {
					e.printStackTrace();
				}
		     }catch(IOException e) {
		    	 e.printStackTrace();
		     }     
		return ret;
	}
}
