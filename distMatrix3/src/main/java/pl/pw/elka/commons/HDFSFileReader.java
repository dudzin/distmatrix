package pl.pw.elka.commons;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

public class HDFSFileReader {

	public ArrayList<String> readFromFile(String path, boolean trim) {
		ArrayList<String> lines = new ArrayList<String>();
		;
		try {

			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(new Path(path))));
			String line;
			line = br.readLine();
			while (line != null) {
				if (trim) {
					lines.add(line.trim());
				} else {
					lines.add(line);
				}
				line = br.readLine();
			}
		} catch (Exception e) {

		}
		return lines;
	}

	public ArrayList<String> readFileIntoArray(String path) {
		ArrayList<String> array = new ArrayList<String>();
		BufferedReader br = null;
		try {
			File folder = new File(path);
			File[] listOfFiles = folder.listFiles();
			for (File file : listOfFiles) {
				if (!file.getName().startsWith(".")) {
					String sCurrentLine;

					br = new BufferedReader(new FileReader(file.getPath()));

					while ((sCurrentLine = br.readLine()) != null) {
						sCurrentLine = sCurrentLine.trim();
						String[] sp = sCurrentLine.split(",");
						for (String s : sp) {
							array.add(s.trim());
						}

					}
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return array;
	}

	public ArrayList<String> readFilesInDir(String path) {
		// System.out.println("path " + path);
		ArrayList<String> array = new ArrayList<String>();
		BufferedReader br = null;
		try {
			File folder = new File(path);
			File[] listOfFiles = folder.listFiles();
			for (File file : listOfFiles) {
				array.add(file.getName());
			}

		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return array;

	}

	public ArrayList<String> readFilesInDir(Path location, Configuration conf)
			throws Exception {
		FileSystem fileSystem = FileSystem.get(location.toUri(), conf);
		FileStatus[] items = fileSystem.listStatus(location);
		if (items == null)
			return new ArrayList<String>();
		ArrayList<String> results = new ArrayList<String>();
		for (FileStatus item : items) {
			// System.out.println("item " + item.getPath());
			// System.out.println(item.getPath().getName());
			results.add(item.getPath().getName());
		}
		return results;
	}

	public void writeFile(Path location, Configuration conf,
			ArrayList<String> list) throws IOException {

		// System.out.println("list size " + list.size());
		FileSystem fileSystem = FileSystem.get(location.toUri(), conf);
		if (fileSystem.exists(location)) {
			fileSystem.delete(location, true);
		}
		OutputStream os = fileSystem.create(location, new Progressable() {
			public void progress() {
				// System.out.println("...bytes written: [ " + 22 + " ]");
			}
		});
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os,
				"UTF-8"));
		for (String string : list) {
			// System.out.println("write line: " + string);
			br.write(string);
			br.newLine();

		}
		br.close();
		fileSystem.close();
	}

	public ArrayList<String> readFromFileByDelim(String path, String delim,
			boolean trim) {
		ArrayList<String> array = new ArrayList<String>();

		File file = new File(path);
		if (file.isFile()) {
			array = readSingleFile(file, delim, trim);
		} else {
			System.out.println("ffaaf:" + file);
			File[] files = file.listFiles();
			if (!(files == null)) {
				for (File f : files) {
					System.out.println("fff:" + f + "  asd " + file);
					if (f.isFile()) {
						System.out.println("fff:" + f + "  asd " + file);
						array.addAll(readSingleFile(f, delim, trim));
					}
				}
			}
		}
		return array;
	}

	private ArrayList<String> readSingleFile(File file, String delim,
			boolean trim) {
		ArrayList<String> array = new ArrayList<String>();
		BufferedReader br = null;
		try {
			if (!file.getName().startsWith(".")) {
				String sCurrentLine;
				br = new BufferedReader(new FileReader(file.getPath()));

				while ((sCurrentLine = br.readLine()) != null) {
					sCurrentLine = sCurrentLine.trim();
					if (!delim.equals("\n")) {
						String[] sp = sCurrentLine.split(delim);
						for (String s : sp) {
							array.add(s.trim());
						}
					} else {
						if (trim) {
							array.add(sCurrentLine.trim());
						} else {
							array.add(sCurrentLine);
						}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return array;
	}
}
