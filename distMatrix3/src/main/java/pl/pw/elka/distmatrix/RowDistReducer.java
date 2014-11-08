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
	private String metric;
	private int divisor;

	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		metric = context.getConfiguration().get("metric");
		if (metric == null) {
			metric = "manhattan";
		}
		divisor = context.getConfiguration().getInt("divisor", 100);
	}

	public void reduce(Text key, Iterable<Text> lines, Context context)
			throws IOException, InterruptedException {

		prepareLists(key, lines);
		String ob1 = "", ob2 = "";
		int dist = -1;
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
				if (metric.equals("manhattan")) {
					dist = calcManh(docWordsList.get(i),
							docWordsList.get(keypos));
				} else if (metric.equals("cosine")) {
					dist = calcCos(docWordsList.get(i),
							docWordsList.get(keypos));
				} else if (metric.equals("euclidean")) {
					dist = calcCos(docWordsList.get(i),
							docWordsList.get(keypos));
				}
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

	public int calcManh(DocWords docWords, DocWords docWords2) {

		int dist = 0;
		for (String string : uniqueWords) {
			dist += Math.abs(docWords.getWordCount(string)
					- docWords2.getWordCount(string));
		}

		return dist;
	}

	public void setUniqueWords(Set<String> uniqueWords) {
		this.uniqueWords = uniqueWords;
	}

	public int calcCos(DocWords v1, DocWords v2) {

		double scal = 0;
		double v1b = 0, v2b = 0;
		double wv1 = 0, wv2 = 0;
		for (String word : uniqueWords) {
			wv1 = v1.getWordCount(word);
			wv2 = v2.getWordCount(word);
			scal += wv1 * wv2;
			v1b += wv1 * wv1;
			v2b += wv2 * wv2;
		}
		v1b = Math.sqrt(v1b);
		v2b = Math.sqrt(v2b);
		int ret = (int) Math.round((1 - scal / (v1b * v2b)) * divisor);
		if (ret == 0)
			ret = 1;
		return ret;

	}

	public void setDivisor(int divisor) {
		this.divisor = divisor;
	}

	public void setMetric(String metric) {
		this.metric = metric;
	}
}
