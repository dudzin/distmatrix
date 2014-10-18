package pl.pw.elka.test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import pl.pw.elka.commons.HDFSFileReader;
import pl.pw.elka.distmatrix.DistMatrixReducer;
import pl.pw.elka.distmatrix.ForbWords;
import pl.pw.elka.distmatrix.PorterStemmer;
import pl.pw.elka.distmatrix.RowDistMapper;
import pl.pw.elka.distmatrix.RowDistReducer;
import pl.pw.elka.distmatrix.Stemmer;
import pl.pw.elka.distmatrix.WordCountMapper;
import pl.pw.elka.distmatrix.WordCountReducer;

public class DistMatrixTest {

	MapDriver<Text, Text, Text, Text> mapDriver;
	MapDriver<LongWritable, Text, Text, Text> rowMapperDriver;
	ReduceDriver<Text, Text, Text, Text> wordcntReducerDriver;
	ReduceDriver<Text, Text, Text, Text> rowReduceDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;
	MapReduceDriver<Text, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	WordCountMapper mapper;
	Text testText;
	Text textName;
	ForbWords forbWords;

	HDFSFileReader fileReader;

	@Before
	public void setUp() {

		mapper = new WordCountMapper();
		// mapper.setStemmer(new PorterStemmer());
		mapDriver = new MapDriver<Text, Text, Text, Text>();
		mapDriver.setMapper(mapper);
		fileReader = new HDFSFileReader();
		WordCountReducer wordcntReducer = new WordCountReducer();
		wordcntReducerDriver = new ReduceDriver<Text, Text, Text, Text>();
		wordcntReducerDriver.setReducer(wordcntReducer);

		RowDistMapper rowMapper = new RowDistMapper();
		rowMapperDriver = new MapDriver<LongWritable, Text, Text, Text>();
		rowMapperDriver.setMapper(rowMapper);

		RowDistReducer rowReducer = new RowDistReducer();
		rowReduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		rowReduceDriver.setReducer(rowReducer);

		DistMatrixReducer reducer = new DistMatrixReducer();
		reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<Text, Text, Text, IntWritable, Text, IntWritable>();
		// mapReduceDriver.setMapper(mapper);
		// mapReduceDriver.setReducer(reducer);

		testText = new Text(
				"Warsaw University of Technology is the best university in a whole universe");
		textName = new Text("test");

	}

	/*
	 * Test the mapper.
	 */
	@Test
	public void testPorterStemmer() {
		Stemmer ps = new PorterStemmer();
		String testText = "strings";

		ps.add(testText);
		ps.stem();
		assertEquals("string", ps.toString());

		testText = "sTRings";
		ps.add(testText);
		ps.stem();
		assertEquals("sTRing", ps.toString());

		testText = "university";
		ps.add(testText);
		ps.stem();
		assertEquals("univers", ps.toString());

	}

	/*
	 * Test the mapper.
	 */

	@Test
	public void testStemmerMapper() {
		mapDriver.getConfiguration().set("stemmer", "Porter");
		mapDriver.addCacheFile("/dict");

		mapDriver.withInput(textName, testText);
		mapDriver.withOutput(textName, new Text("warsaw"));
		mapDriver.withOutput(textName, new Text("univers"));
		mapDriver.withOutput(textName, new Text("of"));
		mapDriver.withOutput(textName, new Text("technolog"));
		mapDriver.withOutput(textName, new Text("is"));
		mapDriver.withOutput(textName, new Text("the"));
		mapDriver.withOutput(textName, new Text("best"));
		mapDriver.withOutput(textName, new Text("univers"));
		mapDriver.withOutput(textName, new Text("in"));
		mapDriver.withOutput(textName, new Text("a"));
		mapDriver.withOutput(textName, new Text("whole"));
		mapDriver.withOutput(textName, new Text("univers"));
		try {
			mapDriver.runTest();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Test
	public void testForbWordsMapper() {

		mapDriver.getConfiguration().set("stemmer", "Porter");
		mapDriver.getConfiguration().set("forbwords", "conf/forbwords");
		mapDriver.getConfiguration().set("regexform", "conf/regexform");
		mapDriver.addCacheFile("/dict");
		mapDriver.addCacheFile("conf/forbwords");
		mapDriver.addCacheFile("conf/regexform");

		mapDriver.withInput(textName, testText);
		mapDriver.withOutput(textName, new Text("warsaw"));
		mapDriver.withOutput(textName, new Text("univers"));
		mapDriver.withOutput(textName, new Text("technolog"));
		mapDriver.withOutput(textName, new Text("best"));
		mapDriver.withOutput(textName, new Text("univers"));
		mapDriver.withOutput(textName, new Text("whole"));
		mapDriver.withOutput(textName, new Text("univers"));

		// try {
		System.out.println("");
		// mapDriver.runTest();
		// } catch (IOException e) {
		// TODO Auto-generated catch block
		// e.printStackTrace();
		// }

	}

	@Test
	public void testWordCntReducer() {

		List<Text> values = new ArrayList<Text>();
		values.add(new Text("hadoop"));
		values.add(new Text("hadoop"));
		values.add(new Text("had"));
		values.add(new Text("had"));
		List<Text> values2 = new ArrayList<Text>();
		values2.add(new Text("hadoop"));
		values2.add(new Text("hadoop"));
		wordcntReducerDriver.getConfiguration().setInt("divisor", 10);
		wordcntReducerDriver.getConfiguration().setInt("minoccurence", 0);
		wordcntReducerDriver.withInput(new Text("file1"), values);
		wordcntReducerDriver.withInput(new Text("file2"), values2);

		wordcntReducerDriver.withOutput(new Text(
				"file1:[file1:1,hadoop:5,had:5]"), new Text(""));
		wordcntReducerDriver.withOutput(new Text("file2:[file2:1,hadoop:10]"),
				new Text(""));

		try {
			wordcntReducerDriver.runTest();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Test
	public void rowDistMapperTest() {

		System.out.println("rowDistMapperTest() ");
		rowMapperDriver.getConfiguration().set("dictionary", "/dict");
		rowMapperDriver.addCacheFile("/dict");

		Text in1 = new Text("file1:[AA:1,BB:4,CC:8]");
		Text in2 = new Text("file2:[AA:4,BB:2]");
		Text in3 = new Text("file3:[AA:8,BB:2,CC:8]");

		Text key1 = new Text("file1");
		Text key2 = new Text("file2");
		Text key3 = new Text("file3");

		Text val1 = new Text("file1:[AA:1,BB:4,CC:8]");
		Text val2 = new Text("file2:[AA:4,BB:2]");
		Text val3 = new Text("file3:[AA:8,BB:2,CC:8]");

		rowMapperDriver.withInput(new LongWritable(0), in1);
		rowMapperDriver.withInput(new LongWritable(0), in2);
		rowMapperDriver.withInput(new LongWritable(0), in3);
		rowMapperDriver.withOutput(key1, val1);
		rowMapperDriver.withOutput(key2, val1);
		rowMapperDriver.withOutput(key3, val1);
		rowMapperDriver.withOutput(key1, val2);
		rowMapperDriver.withOutput(key2, val2);
		rowMapperDriver.withOutput(key3, val2);
		rowMapperDriver.withOutput(key1, val3);
		rowMapperDriver.withOutput(key2, val3);
		rowMapperDriver.withOutput(key3, val3);

		try {
			rowMapperDriver.runTest();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void rowDistReducerTest() throws URISyntaxException {

		Text key1 = new Text("file1");
		Text key2 = new Text("file2");
		Text key3 = new Text("file3");

		List<Text> values1 = new ArrayList<Text>();
		values1.add(new Text("file1:[AA:1,BB:4,CC:8]"));
		values1.add(new Text("file2:[AA:4,BB:2]"));
		values1.add(new Text("file3:[AA:8,BB:2,CC:8]"));

		rowReduceDriver.withInput(key1, values1);
		rowReduceDriver.withInput(key2, values1);
		rowReduceDriver.withInput(key3, values1);
		rowReduceDriver.withOutput(new Text("file1"), new Text("file2:13"));
		rowReduceDriver.withOutput(new Text("file1"), new Text("file3:9"));
		rowReduceDriver.withOutput(new Text("file2"), new Text("file1:13"));
		rowReduceDriver.withOutput(new Text("file2"), new Text("file3:12"));
		rowReduceDriver.withOutput(new Text("file3"), new Text("file1:9"));
		rowReduceDriver.withOutput(new Text("file3"), new Text("file2:12"));

		try {
			rowReduceDriver.runTest();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Test
	public void distMatrixReducerTest() throws URISyntaxException {

		DistributedCache.addCacheFile(new URI("dictionary"),
				mapDriver.getConfiguration());
		reduceDriver.getConfiguration().set("dictionary", "dictionary/dict");

		Text key1 = new Text("file1");
		Text key2 = new Text("file2");
		Text key3 = new Text("file3");

		List<Text> values1 = new ArrayList<Text>();
		values1.add(new Text("file2:9"));
		values1.add(new Text("file3:6"));
		List<Text> values2 = new ArrayList<Text>();
		values2.add(new Text("file1:9"));
		values2.add(new Text("file3:5"));
		List<Text> values3 = new ArrayList<Text>();
		values3.add(new Text("file1:6"));
		values3.add(new Text("file2:5"));

		reduceDriver.withInput(key1, values1);
		reduceDriver.withInput(key2, values2);
		reduceDriver.withInput(key3, values3);
		reduceDriver.withOutput(new Text("file1"), new Text("0,9,6"));
		reduceDriver.withOutput(new Text("file2"), new Text("9,0,5"));
		reduceDriver.withOutput(new Text("file3"), new Text("6,5,0"));

		// try {
		// reduceDriver.runTest();
		// } catch (IOException e) {

		// e.printStackTrace();
		// }

	}

	@Test
	public void readerTest() {

		HDFSFileReader reader = new HDFSFileReader();
		ArrayList<String> list = reader.readFilesInDir("dictionary");
		assertEquals(list.size(), 1);
	}
}
