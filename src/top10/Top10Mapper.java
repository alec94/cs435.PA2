package top10;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Alec on 3/25/2017.
 * gets all TFIDF scores and passes them to the same reducer
 */
public class Top10Mapper extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		//System.out.println("[Mapper] Value: " + value.toString());
		context.write(new Text("Top 10"), value);
	}
}
