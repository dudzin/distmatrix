package pl.pw.elka.distmatrix;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class DocTextInputFormat extends FileInputFormat<Text, Text>{

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit input,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new DocRecordReader();
	}




}
