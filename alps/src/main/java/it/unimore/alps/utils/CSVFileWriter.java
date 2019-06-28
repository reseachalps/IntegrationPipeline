package it.unimore.alps.utils;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.opencsv.CSVWriter;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;

public class CSVFileWriter {

	private String filename;

	public CSVFileWriter(String filename) {
		this.filename = filename;
	}

	private String[] ListStringToArrayString(List<String> list) {

		String[] array = new String[list.size()];

		int index = 0;
		for (String item : list) {
			array[index] = item;
			index++;
		}

		return array;

	}

	public void write(List<List<String>> objects) {

		/*
		 * try ( Writer writer = Files.newBufferedWriter(Paths.get(this.filename)); ) {
		 * 
		 * 
		 * StatefulBeanToCsv beanToCsv = new StatefulBeanToCsvBuilder(writer).build();
		 * //.withQuotechar(CSVWriter.NO_QUOTE_CHARACTER) //.build();
		 * 
		 * try { beanToCsv.write(objects); } catch (CsvDataTypeMismatchException |
		 * CsvRequiredFieldEmptyException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); } } catch (IOException e) { // TODO Auto-generated catch
		 * block e.printStackTrace(); }
		 */

		try (
				Writer writer = Files.newBufferedWriter(Paths.get(this.filename));
				CSVWriter csvWriter = new CSVWriter(writer);
			) {
			for (List<String> obj : objects) {
				csvWriter.writeNext(ListStringToArrayString(obj));
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
