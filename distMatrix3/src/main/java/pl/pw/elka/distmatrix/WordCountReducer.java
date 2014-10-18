package pl.pw.elka.distmatrix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, Text, Text, Text> {

	private ArrayList<Text> vals;
	private static Text ONE = new Text("1");
	private int divisor = 1;
	private int minOccurence = 1;

	// key - word

	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		divisor = context.getConfiguration().getInt("divisor", 1);
		minOccurence = context.getConfiguration().getInt("minoccurence", 1);
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		vals = new ArrayList<Text>();
		int sum = 0;
		for (Text value : values) {
			vals.add(new Text(value));
		}

		Set<Text> uniqueWords = new HashSet<Text>(vals);
		HashMap<Text, IntWritable> hm = new HashMap<Text, IntWritable>();

		for (Text u : uniqueWords) {
			hm.put(u, new IntWritable(0));
		}

		for (Text value : vals) {
			hm.get(value).set(hm.get(value).get() + 1);
		}

		// context.write(ONE, getWordCoords(key, uniqueWords, hm));
		context.write(getWordCoords(key, uniqueWords, hm), new Text(""));

	}

	private Text getWordCoords(Text key, Set<Text> uniqueWords,
			HashMap<Text, IntWritable> hm) {
		String wordsCoords = key + ":[" + key + ":1,";

		double ind = divisor / getTotalWordsCount(uniqueWords, hm);
		double minOccurence = this.minOccurence * ind;
		// System.out.println("divisor: " + divisor + " getT: " +
		// getTotalWordsCount(uniqueWords, hm) + " ind " + ind);
		for (Text uniq : uniqueWords) {
			Double d = (Integer.parseInt("" + hm.get(uniq))) * ind;
			int occurence = d.intValue();
			if (occurence >= minOccurence) {
				wordsCoords += uniq.toString() + ":" + occurence + ",";
			}
		}
		wordsCoords = wordsCoords.substring(0, wordsCoords.length() - 1) + "]";
		// System.out.println("word " + wordsCoords);
		return new Text(wordsCoords);

	}

	private double getTotalWordsCount(Set<Text> uniqueWords,
			HashMap<Text, IntWritable> hm) {
		double count = 0;
		for (Text uniq : uniqueWords) {
			count += Integer.parseInt("" + hm.get(uniq));
		}
		return count;
	}
}