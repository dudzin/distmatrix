package pl.pw.elka.distmatrix;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
	int minval;

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] s = value.toString().split("	");
		context.write(new Text(s[0]), new Text(s[1]));
	}
}
