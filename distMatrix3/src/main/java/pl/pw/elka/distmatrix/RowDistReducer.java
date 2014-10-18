package pl.pw.elka.distmatrix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RowDistReducer extends Reducer<Text, Text, Text, Text> {

	private ArrayList<String> vals;
	private ArrayList<DocWords> docWordsList;
	private Set<String> uniqueWords;

	public void reduce(Text key, Iterable<Text> lines, Context context)
			throws IOException, InterruptedException {

		prepareLists(key, lines);
		String ob1 = "", ob2 = "";
		int dist;
		// write output
		// for (int i = 0; i < docWordsList.size(); i++) {
		// ob1 = docWordsList.get(i).getDocName();

		// for (int j = i + 1; j < docWordsList.size(); j++) {
		// ob2 = docWordsList.get(j).getDocName();
		// dist = calcDist(docWordsList.get(i), docWordsList.get(j));

		// context.write(key, new Text("" + ob2 + ":" + dist));
		// context.write(key, new Text("" + ob1 + ":" + dist));

		// }
		// }
		int keypos = -1;
		for (int i = 0; i < docWordsList.size(); i++) {
			if (docWordsList.get(i).getDocName().equals("" + key)) {
				keypos = i;
			}

		}
		for (int i = 0; i < docWordsList.size(); i++) {
			if (i != keypos) {
				ob2 = docWordsList.get(i).getDocName();
				dist = calcDist(docWordsList.get(i), docWordsList.get(keypos));

				context.write(key, new Text("" + ob2 + ":" + dist));
				// context.write(key, new Text("" + ob1 + ":" + dist));
			}
		}

	}

	private void prepareLists(Text key, Iterable<Text> lines) {

		vals = new ArrayList<String>();
		docWordsList = new ArrayList<DocWords>();
		for (Text line : lines) {
			System.err.println("DDDD: " + key + " line" + line);
			vals.add("" + line);
		}
		for (String val : vals) {
			DocWords docWords = new DocWords(val);
			docWordsList.add(docWords);
		}
		Set<String> keys;
		ArrayList<String> wordsList = new ArrayList<String>();
		for (DocWords doc : docWordsList) {
			keys = doc.getDocWords().keySet();
			for (String string : keys) {
				if (string.length() > 1) {
					wordsList.add(string);
				}
			}
		}
		uniqueWords = new HashSet<String>(wordsList);
		System.out.println("unique words:");
		for (String string : uniqueWords) {
			System.out.println(string);
		}
	}

	private int calcDist(DocWords docWords, DocWords docWords2) {

		int dist = 0;
		for (String string : uniqueWords) {
			dist += Math.abs(docWords.getWordCount(string)
					- docWords2.getWordCount(string));
		}

		return dist;
	}
}
