package it.unimore.alps.sources.openaire;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SinglePublicationFileReader implements Runnable {

	String file;
	Set<String> ids;
	List<Map<String, Object>> results;

	public SinglePublicationFileReader(String file, Set<String> ids, List<Map<String, Object>> results) {
		this.file = file;
		this.ids = ids;
		this.results = results;

	}

	@Override
	public void run() {

		results.addAll(ParserPublication.filterPublicationByProjectId(file, ids));
		System.out.println("Done file: " + file);

	}

}
