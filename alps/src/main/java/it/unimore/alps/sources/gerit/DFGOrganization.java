package it.unimore.alps.sources.gerit;

public class DFGOrganization {
	String germanName;
	String englishName;
	String link;

	String germanType;
	String englishType;
	Integer id;
	Integer zipCode;

	public DFGOrganization(String germanName, String englishName, String link, String germanType, String englishType,
			Integer id, Integer zipCode) {
		super();
		this.germanName = germanName;
		this.englishName = englishName;
		this.link = link;
		this.germanType = germanType;
		this.englishType = englishType;
		this.id = id;
		this.zipCode = zipCode;
	}

	public String getGermanName() {
		return germanName;
	}

	public void setGermanName(String germanName) {
		this.germanName = germanName;
	}

	public String getEnglishName() {
		return englishName;
	}

	public void setEnglishName(String englishName) {
		this.englishName = englishName;
	}

	public String getLink() {
		return link;
	}

	public void setLink(String link) {
		this.link = link;
	}

	public String getGermanType() {
		return germanType;
	}

	public void setGermanType(String germanType) {
		this.germanType = germanType;
	}

	public String getEnglishType() {
		return englishType;
	}

	public void setEnglishType(String englishType) {
		this.englishType = englishType;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public Integer getZipCode() {
		return zipCode;
	}

	public void setZipCode(Integer zipCode) {
		this.zipCode = zipCode;
	}

	@Override
	public String toString() {
		return "DFGOrganization [germanName=" + germanName + ", englishName=" + englishName + ", link=" + link
				+ ", germanType=" + germanType + ", englishType=" + englishType + ", id=" + id + ", zipCode=" + zipCode
				+ "]";
	}

}
