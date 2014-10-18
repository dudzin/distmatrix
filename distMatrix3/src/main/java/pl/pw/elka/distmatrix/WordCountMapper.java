package pl.pw.elka.distmatrix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pl.pw.elka.commons.HDFSFileReader;

public class WordCountMapper extends Mapper<Text, Text, Text, Text> {

	private Text word = new Text();
	private Stemmer stemmer;
	private ForbWords forbWords;
	private String regexForm;
	private HDFSFileReader fileReader;
	String forbWordsFile;
	String regexFormFile;

	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());

		for (Path path : cacheFiles) {
			System.err.println("dist file: " + path);
			if (path.getName().equals("forbwords")) {
				forbWordsFile = path.toString();
			} else if (path.getName().equals("regexform")) {
				regexFormFile = path.toString();
			}
		}

		HDFSFileReader reader = new HDFSFileReader();

		String path = context.getConfiguration().get("forbwords");
		if (path != null) {

			System.err.println("forb: " + path);
			System.err.println(forbWordsFile);
		}

		path = context.getConfiguration().get("regexform");
		if (path != null) {

			System.err.println("regexform: " + path);
			System.err.println(regexFormFile);
		}

		initConf(context);

	}

	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {

		// initConf(context);
		List<String> words = initWords(value);

		for (String w : words) {
			w = processWord(w);

			if (w != null && !w.equals("")) {
				word.set(w);
				context.write(key, new Text(word));
			}

		}
	}

	private void initConf(Context context) {

		fileReader = new HDFSFileReader();

		String stemmerClass = context.getConfiguration().get("stemmer");
		if (stemmerClass == null) {
		} else if (stemmerClass.equals("Porter")) {
			stemmer = new PorterStemmer();
		}

		// String forbWordsFile = context.getConfiguration().get("forbwords");
		if (forbWordsFile != null) {
			forbWords = new ForbWords();
			forbWords.setWords(
			// fileReader.readFromFileB(// forbWordsFile
			// "/forbwords", true)
					fileReader.readFromFileByDelim(forbWordsFile, "\n", false));
			System.err.println("forb size " + forbWords.getWords());
		}

		// String regexFormFile = context.getConfiguration().get("regexform");
		if (regexFormFile != null) {
			System.err.println("regexFormFile " + regexFormFile);
			regexForm = fileReader.// readFromFile("regexform" // regexFormFile
					// ,// + "/regexform",
					// false
					readFromFileByDelim(regexFormFile, "\n", false).get(0);
			System.err.println("regexForm " + regexForm);
		}

	}

	private List<String> initWords(Text value) {

		String[] words = value.toString().split(" ");
		List<String> list = new ArrayList<String>();

		for (String s : words) {
			if (regexForm != null) {
				s = s.replaceAll(regexForm, "");
				if (s != null && s.length() > 0) {
					list.add(s.toLowerCase());
				}
			} else {
				if (s != null && s.length() > 0) {
					list.add(s.toLowerCase());
				}
			}
		}
		return list;
	}

	private String processWord(String w) {

		if (stemmer != null) {
			stemmer.add(w.replaceAll("\"", "").replaceAll("\'", ""));
			stemmer.stem();
			w = stemmer.toString();
		}
		if (forbWords != null) {
			if (forbWords.isWorb(w)) {
				w = null;
			}
		}
		return w;
	}

}
