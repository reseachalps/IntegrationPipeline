package it.unimore.alps.integrator;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Enumeration;

import javax.swing.JOptionPane;

public class NutsConverter {
	
	private int id;
	private double lat;
	private double lon;
	private String geom;
	private String nuts3;
	private String nuts2;
	private String nuts1;
	private static final int SRID = 4326;
	
	//private static final String url = "jdbc:postgresql://localhost/nuts_prova";
	private static final String url = "jdbc:postgresql://localhost:5432/rsa_nuts";
	//private static final String user="postgres";
	private static final String user="paolo";
	//private static final String password="";
	private static final String password="research+alps+paolo";
	public static final String TABLE_NUTS="nuts_levl_3";
	public static final String COLUMN_NUTS_ID="nuts_id";
	public static final String COLUMN_NUTS_GEOM="geom";
	
	private Connection conn;
	
	
	
	public double getLat() {
		return lat;
	}
	public void setLat(double lat) {
		this.lat = lat;
	}
	public double getLon() {
		return lon;
	}
	public void setLon(double lon) {
		this.lon = lon;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getGeom() {
		return geom;
	}
	public void setGeom(String geom) {
		this.geom = geom;
	}
	public int getSRID() {
		return SRID;
	}
	public String getNuts3() {
		return nuts3;
	}
	public void setNuts3(String nuts3) {
		this.nuts3 = nuts3;
	}
	public String getNuts2() {
		return nuts2;
	}
	public void setNuts2(String nuts3) {
		if(nuts3==null)
			return;
		else {
			String nuts=nuts3.substring(0, 4);
			this.nuts2 = nuts;
		}
	}
	public String getNuts1() {
		return nuts1;
	}
	
	
	public void setNuts1(String nuts3) {
		if(nuts3==null)
			return;
		else {
			String nuts=nuts3.substring(0, 3);
			this.nuts1 = nuts;
		}
	}
	
	
	public boolean open() {		
		
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.println("CONNECTION ACTIVATED");
		
		try {
			conn = DriverManager.getConnection(url, user, password);
			return true;
		} catch (SQLException e) {
			System.out.println("Couldn't connect to database: " + e.getMessage());
			return false;
		}
	}
	
	public void close() {
		try {
			if(conn != null) {
				conn.close();
			}
		} catch(SQLException e) {
			System.out.println("Couldn't close connection: " + e.getMessage());
		}
	}
	
	
	
	public void setNuts(){

		String queryString = "SELECT " + COLUMN_NUTS_ID +
				" FROM  public." +'"'+ TABLE_NUTS + '"'+
				" WHERE ST_Intersects(ST_SetSRID(ST_MakePoint(?,?),?), geom) = true ";



		try {

			PreparedStatement pStatement=conn.prepareStatement(queryString);
			//System.out.println(lat + ", "+ lon);


			pStatement.setDouble(1, lon);
			pStatement.setDouble(2, lat);
			pStatement.setInt(3, SRID);


			ResultSet results=pStatement.executeQuery();
			
			while(results.next()) {

				setNuts3(results.getString(COLUMN_NUTS_ID));


			}
			if(getNuts3()==null) {
				setNutsDW();
			}
			setNuts2(getNuts3());
			setNuts1(getNuts3());


		}catch(SQLException e){
			e.printStackTrace();
			System.out.println("SetNuts query failed: " + e.getMessage());

		}

	}

	public void setNutsDW() {
		String joinString = "SELECT " + COLUMN_NUTS_ID +
				" FROM  public." +'"'+ TABLE_NUTS + '"'+
				" WHERE ST_DWithin(ST_SetSRID(ST_MakePoint(?,?),?)::geography, geom::geography , 3000)";


		try {PreparedStatement pStatement= conn.prepareStatement(joinString);


		pStatement.setDouble(1, lon);
		pStatement.setDouble(2, lat);
		pStatement.setInt(3, SRID);
		ResultSet results=pStatement.executeQuery();

		while(results.next()) {
			setNuts3(results.getString(COLUMN_NUTS_ID));
			

		}



		}catch(SQLException e){
			e.printStackTrace();
			System.out.println("SetNutsDW query failed: " + e.getMessage());


		} 


	}

	

}
