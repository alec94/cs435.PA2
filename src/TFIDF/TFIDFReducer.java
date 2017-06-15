package TFIDF;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		ArrayList<String> cache = new ArrayList<>(); //store "author|TF"

		Configuration configuration = context.getConfiguration();
		long authorCount = Long.parseLong(configuration.get("authorCount"));
		double usages = 0.0;
		double IDF, TF, TFIDF;

		//COUNT number of works unigram appears in
		for (Text value : values) {
			usages++;
			cache.add(value.toString());
		}

		for (String string : cache) { // calculate TFIDF
			IDF = Math.log10(authorCount / usages);
			String[] split = string.split("\\|");
			TF = Double.parseDouble(split[1]);
			TFIDF = TF * IDF;
			context.write(key, new Text(split[0] + "|" + TFIDF + " " + IDF));
		}


	}
}
