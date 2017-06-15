package TF;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TFReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	//input: <author|unigram; 1>
	//output: <author|unigram;	termFrequencyFij>
	public void reduce(Text key, Iterable<IntWritable> values,
					   Context context
	) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}

		context.write(key, new IntWritable(sum));
	}
}