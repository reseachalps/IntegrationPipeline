package it.unimore.alps.sources.gerit;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class ParserDFG {

	public static void main(String[] args) {

		ParserDFG p = new ParserDFG();

		String fileName = "/Users/paolosottovia/Downloads/institutionen_gerit___.xlsx";

		List<DFGOrganization> orgs = p.readInitialFile(fileName);

		List<Integer> ids = new ArrayList<>();

		for (DFGOrganization org : orgs) {
			System.out.println("Organization org: " + org.toString());
			ids.add(org.getId());
		}

		Crawler c = new Crawler();
		for (Integer id : ids) {

			if (id.intValue() >= 427977617) {

				System.out.println("ID ANALIZED: " + id);

				c.downloadPageID(id);
			}
		}

	}

	public List<DFGOrganization> readInitialFile(String file) {
		File excelFile = new File(file);
		FileInputStream fis;
		XSSFWorkbook workbook;

		List<DFGOrganization> organizations = new ArrayList<>();

		try {
			fis = new FileInputStream(excelFile);

			// we create an XSSF Workbook object for our XLSX Excel File
			workbook = new XSSFWorkbook(fis);
			// we get first sheet
			XSSFSheet sheet = workbook.getSheetAt(0);

			// we iterate on rows
			Iterator<Row> rowIt = sheet.iterator();

			boolean firstLine = true;

			while (rowIt.hasNext()) {

				List<String> line = new ArrayList<>();

				Row row = rowIt.next();

				// iterate on cells for the current row
				Iterator<Cell> cellIterator = row.cellIterator();
				if (!firstLine) {
					for (int i = 0; i < 10; i++) {
						Cell cell = row.getCell(i);
						if (cell != null) {
							line.add(cell.toString());
							System.out.println("" + cell.toString());
						} else {
							line.add("");
							System.out.println("EMPTY CELL");
						}
					}
				}

				if (!firstLine) {
					for (String el : line) {
						System.out.print(el + ";");
					}
					System.out.println(" ");
				}

//				while (cellIterator.hasNext()) {
//					Cell cell = cellIterator.next();
//
//					if (!firstLine) {
//						System.out.print(cell.toString() + ";");
//						line.add(cell.toString());
//					}
//				}

				if (!firstLine) {
					System.out.println();
				}
				firstLine = false;

				if (line.size() > 0) {

					String germanName = line.get(0);
					String englishName = line.get(1);
					String link = line.get(2);

					String germanType = line.get(3);// 3
					String englishType = line.get(4);// 4
					Integer id = (int) Double.parseDouble(line.get(5));

					Integer zipCode = 00000;

//					if (!line.get(8).trim().equals("")) {
//						zipCode = (int) Double.parseDouble(line.get(8));
//					}

					// Integer zipCode = 00000;
					DFGOrganization org = new DFGOrganization(germanName, englishName, link, germanType, englishType,
							id, zipCode);

					System.out.println();

					organizations.add(org);

				}

			}

			workbook.close();
			fis.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

		}

		return organizations;

	}

}
