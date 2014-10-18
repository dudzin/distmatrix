package pl.pw.elka.distmatrix;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pl.pw.elka.commons.HDFSFileReader;

public class RowDistMapper extends Mapper<LongWritable, Text, Text, Text> {

	private ArrayList<String> dict;

	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);

		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
				.getConfiguration());

		for (Path path : cacheFiles) {
			System.out.println("dist file: " + path);
		}

		HDFSFileReader reader = new HDFSFileReader();

		String path = context.getConfiguration().get("dictionary");
		path = "." + path.substring(path.lastIndexOf("/"));
		dict = reader.readFromFileByDelim(path, ",", false);
	}

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] s = value.toString().split("	");

		for (String d : dict) {
			context.write(new Text(d), new Text(s[0]));
		}

	}

}
