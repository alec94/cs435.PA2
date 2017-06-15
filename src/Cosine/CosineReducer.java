package Cosine;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CosineReducer extends Reducer<Text, DoubleWritable, Text, Text> {
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		context.write(key, new Text(values.iterator().next() + ""));
	}
}
