package TFIDF;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class TFIDFMapper extends Mapper<Text, Text, Text, Text> {
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
	}
}


