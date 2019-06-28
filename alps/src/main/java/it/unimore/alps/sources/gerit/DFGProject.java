package it.unimore.alps.sources.gerit;

import org.json.JSONObject;

public class DFGProject {

	private String id;
	private String name;
	private String programme;
	private String description;
	private String url;
	private String duration;
	private String startDate;
	private String endDate;

	public DFGProject(String id, String name, String programme, String description, String url, String duration,
			String startDate, String endDate) {
		super();
		this.id = id;
		this.name = name;
		this.programme = programme;
		this.description = description;
		this.url = url;
		this.duration = duration;
		this.startDate = startDate;
		this.endDate = endDate;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getProgramme() {
		return programme;
	}

	public void setProgramme(String programme) {
		this.programme = programme;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getDuration() {
		return duration;
	}

	public void setDuration(String duration) {
		this.duration = duration;
	}

	public String getStartDate() {
		return startDate;
	}

	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}

	public String getEndDate() {
		return endDate;
	}

	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}

	public JSONObject toJSONOBject() {
		JSONObject obj = new JSONObject();
		obj.put("id", this.id);
		obj.put("name", this.name);
		obj.put("programme", this.programme);
		obj.put("description", this.description);
		obj.put("url", this.url);
		obj.put("duration", this.duration);
		obj.put("startDate", this.startDate);
		obj.put("endDate", this.endDate);

		return obj;

	}

	public static DFGProject fromJsonToDFGProject(String json) {

		JSONObject obj = new JSONObject(json);
		String id = obj.getString("id");

		String name = obj.getString("name");
		String programme = obj.getString("programme");
		String description = obj.getString("description");
		String url = obj.getString("url");
		String duration = obj.getString("duration");
		String startDate = obj.getString("startDate");
		String endDate = "";
		if (obj.has("endDate")) {
			endDate = obj.getString("endDate");
		}

		DFGProject p = new DFGProject(id, name, programme, description, url, duration, startDate, endDate);
		return p;

	}

	@Override
	public String toString() {
		return "DFGProject [id=" + id + ", name=" + name + ", programme=" + programme + ", description=" + description
				+ ", url=" + url + ", duration=" + duration + ", startDate=" + startDate + ", endDate=" + endDate + "]";
	}

}
