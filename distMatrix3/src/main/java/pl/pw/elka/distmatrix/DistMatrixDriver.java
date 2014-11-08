package pl.pw.elka.distmatrix;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import pl.pw.elka.commons.HDFSFileReader;

public class DistMatrixDriver extends Configured implements Tool {

	// sudo -u hdfs hadoop jar distMatrix-0.0.3-SNAPSHOT.jar
	// pl.pw.elka.distmatrix.DistMatrixDriver -D workdir=stages

	/**
	 * To do 1. obsługiwanie przypadków gdy z wordcount nie wychodzą, żadne
	 * słowa
	 * */
	ArrayList<String> list;
	HDFSFileReader reader;
	Configuration conf;
	Configuration conf2; // new Configuration();
	Configuration conf3;
	Path inputPath;
	Path wordcntOutputDir;
	Path rowDistMxDir;
	Path outputDir;
	Job job1, job2, job3;
	boolean[] steps;

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new DistMatrixDriver(),
				args);
		System.exit(res);

	}

	@Override
	public int run(String[] args) throws Exception {
		long startTime = System.nanoTime();

		conf = this.getConf();
		conf2 = this.getConf(); // new Configuration();
		conf3 = this.getConf();

		readSteps();

		initConf();
		createDictionary();
		createJobs();

		boolean result = false;
		if (steps[0]) {
			System.out.println("step 1: Wordcount:");
			result = job1.waitForCompletion(true);
			if (!result) {
				return result ? 0 : 1;
			}
		}

		long interTime = System.nanoTime();
		System.out.println("\n\n Step 1 Execution Time: "
				+ ((interTime - startTime) / 1000000000) + " sec \n");
		if (steps[1]) {
			System.out.println("step 2: Row Distance Matrix:");
			result = job2.waitForCompletion(true);
			if (!result) {
				return result ? 0 : 1;
			}
		}
		long inter2Time = System.nanoTime();
		System.out.println("\n\n Step 2 Execution Time: "
				+ ((inter2Time - interTime) / 1000000000) + " sec \n");

		if (steps[2]) {
			System.out.println("step 3: Distance Matrix:");
			result = job3.waitForCompletion(true);
		}

		long stopTime = System.nanoTime();
		System.out.println("\n\n Step 3 Execution Time: "
				+ ((stopTime - inter2Time) / 1000000000) + " sec \n");
		System.out.println("\n Full Execution Time: "
				+ ((stopTime - startTime) / 1000000000) + " sec \n");
		System.out.println("Step 1 Execution Time: "
				+ ((interTime - startTime) / 1000000000) + " sec \n");
		System.out.println("Step 2 Execution Time: "
				+ ((inter2Time - interTime) / 1000000000) + " sec \n");
		System.out.println("Step 3 Execution Time: "
				+ ((stopTime - inter2Time) / 1000000000) + " sec \n");
		return result ? 0 : 1;

	}

	private void readSteps() {
		steps = new boolean[3];
		String st = conf.get("steps");

		if (st == null) {
			steps = new boolean[] { true, true, true };
			return;
		}

		if (st.contains("1")) {
			steps[0] = true;
		} else {
			steps[0] = false;
		}
		if (st.contains("2")) {
			steps[1] = true;
		} else {
			steps[1] = false;
		}
		if (st.contains("3")) {
			steps[2] = true;
		} else {
			steps[2] = false;
		}

	}

	private void initConf() throws IntializatonException {

		conf.set("stemmer", "Porter");

		conf.setInt("divisor", conf.getInt("divisor", 100));
		conf.setInt("minoccurence", conf.getInt("minoccurence", 2));
		conf.set("metric", conf.get("metric", "cosine"));

		if (steps[0]) {
			initWorkingDirectory(conf);
		}
		if (steps[1]) {
			initWorkingDirectory(conf2);
		}
		if (steps[2]) {
			initWorkingDirectory(conf3);
		}

		inputPath = new Path(conf.get("input"));
		wordcntOutputDir = new Path(conf.get("wordcntOutput"));
		rowDistMxDir = new Path(conf.get("rowdistmxOutput"));
		outputDir = new Path(conf.get("output"));
		printParams();
	}

	private void initWorkingDirectory(Configuration conf)
			throws IntializatonException {

		String workdir = conf.get("workdir");

		if (workdir.equals("")) {
			System.out.println("no workdir specified");
			throw new IntializatonException();
		}
		try {
			addProperty(workdir, "input", conf);
			addProperty(workdir, "forbwords", conf);
			addProperty(workdir, "regexform", conf);

			DistributedCache.addCacheFile(new URI(conf.get("forbwords")), conf);
			DistributedCache.addCacheFile(new URI(conf.get("regexform")), conf);

			prepareFolder(workdir, "wordcntOutput", conf, steps[0]);
			prepareFolder(workdir, "rowdistmxOutput", conf, steps[1]);
			prepareFolder(workdir, "output", conf, steps[2]);

		} catch (Exception e) {

			IntializatonException ex = new IntializatonException();
			System.out.println(ex.getMessage());
			System.out.println(e.getMessage());
			throw ex;
		}
		conf.set("dictionary", conf.get("workdir") + "/dictionary");
		conf.set("clusters", conf.get("workdir") + "/clusters");
		conf.set(workdir + "/wordcountOutput", "wordcntOutput");
		conf.set(workdir + "/rowdistmxOutput", "rowdistmxOutput");

	}

	private void prepareFolder(String workdir, String name, Configuration conf,
			boolean delete) throws IOException, URISyntaxException {
		String property = workdir + "/" + name;
		FileSystem fileSystem = FileSystem.get(new URI(workdir), conf);
		if (delete && fileSystem.isDirectory(new Path(property))
				|| fileSystem.isFile(new Path(property))) {
			fileSystem.delete(new Path(property), true);
		}

		conf.set(name, property);
	}

	private void addProperty(String workdir, String name, Configuration conf)
			throws IOException, URISyntaxException {
		String property = workdir + "/" + name;
		FileSystem fileSystem = FileSystem.get(new URI(workdir), conf);
		if (!fileSystem.isDirectory(new Path(property))
				&& !fileSystem.isFile(new Path(property))) {
			System.out.println("no " + property + " directory or file");
		} else {
			conf.set(name, property);
		}
	}

	private void printParams() {
		System.out.println("params ");
		System.out.println("forbwords        : " + conf.get("forbwords"));
		System.out.println("regexform        : " + conf.get("regexform"));
		System.out.println("metric           : " + conf.get("metric"));
		System.out.println("input            : " + conf.get("input"));
		System.out.println("dictionary       : " + conf.get("dictionary"));
		System.out.println("wordcntOutput    : " + conf.get("wordcntOutput"));
		System.out.println("rowdistmxOutput  : " + conf.get("rowdistmxOutput"));

		System.out.println("output           : " + conf.get("output"));
		System.out.println("clusters         : " + conf.get("clusters"));
		System.out.println("divisor          : " + conf.get("divisor"));
		System.out.println("minoccurence     : " + conf.get("minoccurence"));

	}

	private void createDictionary() throws Exception {
		reader = new HDFSFileReader();
		String dip = conf.get("input");
		System.out.println("read " + dip);
		list = reader.readFilesInDir(new Path(dip), conf);

		String dictionary = conf.get("dictionary");
		reader.writeFile(new Path(dictionary), conf, list);
		DistributedCache.addCacheFile(new URI(dictionary), conf);
		DistributedCache.addCacheFile(new URI(dictionary), conf2);
		DistributedCache.addCacheFile(new URI(dictionary), conf3);

		ArrayList<String> clusters = new ArrayList<String>();
		for (int i = 0; i < list.size(); i++) {
			clusters.add("" + i);
		}
		String clustersPath = conf.get("clusters");
		reader.writeFile(new Path(clustersPath), conf, clusters);
	}

	private void createJobs() throws IOException {
		if (steps[0]) {
			createJob1();
		}
		if (steps[1]) {
			createJob2();
		}
		if (steps[2]) {
			createJob3();
		}
	}

	private void createJob1() throws IOException {
		job1 = new Job(conf, "WordCount");
		job1.setJarByClass(DistMatrixDriver.class);
		job1.setJarByClass(WordCountReducer.class);
		// job.setJarByClass(RowDistReducer.class); //
		job1.setJarByClass(WordCountMapper.class);

		job1.setMapperClass(WordCountMapper.class);
		// job.setCombinerClass(WordCountReducer.class);
		job1.setReducerClass(WordCountReducer.class);
		// job.setNumReduceTasks(1);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, inputPath);
		job1.setInputFormatClass(DocTextInputFormat.class);

		FileOutputFormat.setOutputPath(job1, wordcntOutputDir);
		job1.setOutputFormatClass(TextOutputFormat.class);
	}

	private void createJob2() throws IOException {

		job2 = new Job(conf2, "RowDistMx");
		job2.setJarByClass(RowDistMapper.class);
		job2.setJarByClass(RowDistReducer.class);
		job2.setMapperClass(RowDistMapper.class);
		job2.setReducerClass(RowDistReducer.class);
		job2.setNumReduceTasks(1);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, wordcntOutputDir);
		job2.setInputFormatClass(TextInputFormat.class);
		FileOutputFormat.setOutputPath(job2, rowDistMxDir);
		job2.setOutputFormatClass(TextOutputFormat.class);

	}

	private void createJob3() throws IOException {

		job3 = new Job(conf3, "DistMx");

		job3.setJarByClass(DistMatrixMapper.class);
		job3.setJarByClass(DistMatrixReducer.class);
		job3.setMapperClass(DistMatrixMapper.class);
		job3.setReducerClass(DistMatrixReducer.class);
		// job3.setNumReduceTasks(1);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job3, rowDistMxDir);
		job3.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job3, outputDir);
		job3.setOutputFormatClass(TextOutputFormat.class);

	}
	/*
	 * private void createDictionary(Configuration conf) throws
	 * FileNotFoundException, UnsupportedEncodingException { String path =
	 * conf.get("input"); path = path.substring(0, path.lastIndexOf("/")) +
	 * "/dir"; PrintWriter writer = new PrintWriter(path, "UTF-8");
	 * writer.println("The first line"); writer.println("The second line");
	 * 
	 * for (String file : list) { writer.print(file + ","); } writer.close(); }
	 */
}
