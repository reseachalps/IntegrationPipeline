package it.unimore.alps.integrator;

public class CorrectUrl {
	private String link;
	private int code;
	private boolean isRedirected;
	public CorrectUrl() {
		super();
		link="";
		code=0;
		isRedirected=false;
	}
	public String getLink() {
		return link;
	}
	public void setLink(String link) {
		this.link = link;
	}
	public int getCode() {
		return code;
	}
	public void setCode(int code) {
		this.code = code;
	}
	public boolean isRedirected() {
		return isRedirected;
	}
	public void setRedirected(boolean isRedirected) {
		this.isRedirected = isRedirected;
	}
}
