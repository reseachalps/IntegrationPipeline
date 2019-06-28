package it.unimore.alps.deeplearning;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import it.unimore.alps.deduplication.DeduplicatorUtilTest;

public class DataPreparation {

	public static void main(String[] args) {
		String inputFile = "/Users/paolosottovia/Downloads/correspondenceOrganizations_labelQgrams.tsv";
		String outputFile = "/Users/paolosottovia/Downloads/matches.csv";

		DataPreparation prep = new DataPreparation();
		try {
			prep.createMatchFile(inputFile, outputFile);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void createMatchFile(String originalMatchFile, String outpathMatchFile) throws IOException {
		DeduplicatorUtilTest dutil = new DeduplicatorUtilTest();

		Set<Set<String>> correspondecesOrgs = dutil.readCorrespondences(originalMatchFile);
		List<String> lines = new ArrayList<>();
		lines.add("ltable_id,rtable_id,label");

		for (Set<String> corr : correspondecesOrgs) {
			List<String> corrList = new ArrayList<>();
			corrList.addAll(corr);
			if (corrList.size() > 1) {
				for (int i = 0; i < corrList.size() - 1; i++) {
					String id1 = corrList.get(i);
					String id2 = corrList.get(i + 1);

					String line = id1 + "," + id2 + "," + 1;
					lines.add(line);
				}
			}

		}

		Writer out = null;
		try {
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outpathMatchFile), "UTF-8"));
			for (String line : lines) {
				out.write(line + "\n");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			out.close();
		}

	}

}
