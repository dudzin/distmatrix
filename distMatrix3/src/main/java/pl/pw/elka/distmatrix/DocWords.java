package pl.pw.elka.distmatrix;

import java.util.HashMap;

public class DocWords {
	private String docName;
	private HashMap<String, Integer> docWords;

	public DocWords() {
		docWords = new HashMap<String, Integer>();
	}

	public DocWords(String val) {
		String words;
		docWords = new HashMap<String, Integer>();
		docName = val.substring(0, val.indexOf("[") - 1);
		words = val.substring(val.indexOf("[") + 1, val.length() - 1);
		String[] word = words.split(",");

		for (String string : word) {
			String[] split = string.split(":");
			addRow(split[0], Integer.parseInt(split[1]));
		}

	}

	public int getWordCount(String word) {
		if (docWords.get(word) == null) {
			return 0;
		}
		return docWords.get(word);
	}

	public void addRow(String key, int val) {
		docWords.put(key, val);
	}

	public String getDocName() {
		return docName;
	}

	public void setDocName(String docName) {
		this.docName = docName;
	}

	public HashMap<String, Integer> getDocWords() {
		return docWords;
	}

	public void setDocWords(HashMap<String, Integer> docWords) {
		this.docWords = docWords;
	}

	public String toString() {
		String message = "docName: " + docName + "\n";
		message += docWords.toString();
		return message;

	}

}
