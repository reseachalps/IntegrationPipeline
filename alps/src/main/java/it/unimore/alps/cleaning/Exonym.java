package it.unimore.alps.cleaning;

public class Exonym {

	Integer alternateNameId;// : the id of this alternate name, int
	Integer geonameid; // : geonameId referring to id in table 'geoname', int
	String isolanguage; // : iso 639 language code 2- or 3-characters; 4-characters 'post' for postal
						// codes and 'iata','icao' and faac for airport codes, fr_1793 for French
						// Revolution names, abbr for abbreviation, link to a website (mostly to
						// wikipedia), wkdt for the wikidataid, varchar(7)
	String alternateName; // : alternate name or name variant, varchar(400)
	Integer isPreferredName; // : '1', if this alternate name is an official/preferred name
	Integer isShortName; // : '1', if this is a short name like 'California' for 'State of California'
	Integer isColloquial; // : '1', if this alternate name is a colloquial or slang term. Example: 'Big
							// Apple' for 'New York'.
	Integer isHistoric; /// : '1', if this alternate name is historic and was used in the past. Example
						/// 'Bombay' for 'Mumbai'.
	String from; // : from period when the name was used
	String to; // : to period when the name was used

	String country;

	public Exonym(Integer alternateNameId, Integer geonameid, String isolanguage, String alternateName,
			Integer isPreferredName, Integer isShortName, Integer isColloquial, Integer isHistoric, String from,
			String to, String country) {
		super();
		this.alternateNameId = alternateNameId;
		this.geonameid = geonameid;
		this.isolanguage = isolanguage;
		this.alternateName = alternateName;
		this.isPreferredName = isPreferredName;
		this.isShortName = isShortName;
		this.isColloquial = isColloquial;
		this.isHistoric = isHistoric;
		this.from = from;
		this.to = to;
		this.country = country;
	}

	@Override
	public String toString() {
		return this.alternateNameId + "\t" + this.geonameid + "\t" + this.isolanguage + "\t" + this.alternateName + "\t"
				+ this.isPreferredName + "\t" + this.isShortName + "\t" + this.isColloquial + "\t" + this.isHistoric
				+ "\t" + this.from + "\t" + this.to + "\t" + this.country;
	}

	public Integer getAlternateNameId() {
		return alternateNameId;
	}

	public void setAlternateNameId(Integer alternateNameId) {
		this.alternateNameId = alternateNameId;
	}

	public Integer getGeonameid() {
		return geonameid;
	}

	public void setGeonameid(Integer geonameid) {
		this.geonameid = geonameid;
	}

	public String getIsolanguage() {
		return isolanguage;
	}

	public void setIsolanguage(String isolanguage) {
		this.isolanguage = isolanguage;
	}

	public String getAlternateName() {
		return alternateName;
	}

	public void setAlternateName(String alternateName) {
		this.alternateName = alternateName;
	}

	public Integer getIsPreferredName() {
		return isPreferredName;
	}

	public void setIsPreferredName(Integer isPreferredName) {
		this.isPreferredName = isPreferredName;
	}

	public Integer getIsShortName() {
		return isShortName;
	}

	public void setIsShortName(Integer isShortName) {
		this.isShortName = isShortName;
	}

	public Integer getIsColloquial() {
		return isColloquial;
	}

	public void setIsColloquial(Integer isColloquial) {
		this.isColloquial = isColloquial;
	}

	public Integer getIsHistoric() {
		return isHistoric;
	}

	public void setIsHistoric(Integer isHistoric) {
		this.isHistoric = isHistoric;
	}

	public String getFrom() {
		return from;
	}

	public void setFrom(String from) {
		this.from = from;
	}

	public String getTo() {
		return to;
	}

	public void setTo(String to) {
		this.to = to;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

}
