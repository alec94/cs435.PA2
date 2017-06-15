package combine;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CombineMapper extends Mapper<Text, Text, Text, Text> {
	// reads in TFIDF scores from offline stage
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
	}
}