package pl.pw.elka.distmatrix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pl.pw.elka.commons.HDFSFileReader;

public class DistMatrixReducer extends Reducer<Text, Text, Text, Text> {

	private ArrayList<String> dict;
	private HashMap<String, Integer> files;

	public void setup(Context context) throws IOException, InterruptedException {

		super.setup(context);
		HDFSFileReader reader = new HDFSFileReader();
		files = new HashMap<String, Integer>();
		// String path = context.getConfiguration().get("dictionary");
		// dict = reader.readFromFile(path
		// "stages/tmp/dictionary"
		// + path.substring(path.lastIndexOf("/")
		// , true);

		// dict = reader.readFilesInDir("./f");

		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());
		String dictpath = "";
		for (Path path : cacheFiles) {
			System.err.println("dist file: " + path);
			if (path.getName().equals("dictionary")) {
				dictpath = path.toString();
			}
		}
		dict = reader.readFromFileByDelim(dictpath, "\n", false);
	}

	public void reduce(Text key, Iterable<Text> lines, Context context)
			throws IOException, InterruptedException {

		String[] s;
		for (Text text : lines) {
			s = text.toString().split(":");
			System.out.println(s[0] + " line: " + s[1]);
			files.put(s[0], new Integer(s[1]));
		}

		String output = "";
		for (String d : dict) {
			if (d.equals(key.toString())) {
				output += "0,";
			} else {
				output += files.get(d) + ",";
			}
		}

		output = output.substring(0, output.length() - 1);
		context.write(key, new Text(output));
		// for (String d : dict) {
		// context.write(key, new Text(d));
		// }

	}
}
