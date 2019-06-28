package it.unimore.alps.sources.openaire;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Utils {

	public static List<String> retrievePaths(String folderPath, String prefix) {
		List<String> paths = new ArrayList<>();
		File folder = new File(folderPath);
		if (folder.isDirectory()) {

			File[] files = folder.listFiles();
			for (File file : files) {
				if (file.getName().startsWith(prefix)) {
					System.out.println("" + file.getAbsolutePath());
					paths.add(file.getAbsolutePath());
				}
			}
			return paths;

		} else {
			throw new RuntimeException("Error! Input path is not a folder! ");
		}
	}

	public static void printStructure(Map<String, Object> structure, int level) {
		for (Map.Entry<String, Object> entry : structure.entrySet()) {
			Object object = entry.getValue();
			String name = entry.getKey();
			System.out.println("" + getTab(level) + name);
			if (object == null) {
				System.out.println(getTab(level) + "\tnull");
			} else {
				if (object instanceof String) {
					String value = (String) object;
					System.out.println("\t" + getTab(level) + value);
				} else if (object instanceof Map<?, ?>) {
					Map<String, Object> value = (Map<String, Object>) object;
					printStructure(value, level + 1);
				} else if (object instanceof List<?>) {
					List<Map<String, Object>> values = (List<Map<String, Object>>) object;
					for (Map<String, Object> value : values) {
						printStructure(value, level + 1);
					}

				}
			}
		}
	}

	private static String getTab(int number) {
		String tabs = "\t";
		for (int i = 0; i < number; i++) {
			tabs += "\t";
		}
		return tabs;
	}

}
