package authorCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import util.OfflineDriver.authorCount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AuthorCountReducer extends Reducer<Text, AuthorCountWritable, Text, Text> {
	public void reduce(Text key, Iterable<AuthorCountWritable> values, Context context) throws IOException, InterruptedException {
		HashMap<String, Integer> cache = new HashMap<String, Integer>();
		int max = Integer.MIN_VALUE;
		int frequency = -1;
		for (AuthorCountWritable val : values) {  //cannot iterate twice, cache it
			frequency = val.getFrequency();
			if (frequency > max) {
				max = frequency;
			}

			cache.put(val.getUnigram().toString(), frequency);
		}

		for (Map.Entry<String, Integer> entry : cache.entrySet()) {
			context.write(new Text(entry.getKey()), new Text(key.toString() + "|" + (double) entry.getValue() / max));
		}

		context.getCounter(authorCount.COUNT).increment(1); // COUNT number of authors
	}
}
