package combine;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.LineReader;
import util.Util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

public class CombineReducer extends Reducer<Text, Text, Text, Text> {

	private HashMap<String, Double> cache = new HashMap<String, Double>();

	@Override
	public void setup(Context context) throws IOException {
		// make unknown TF scores available to all mappers
		FileSystem fileSystem = FileSystem.get(context.getConfiguration());

		if (!fileSystem.exists(Util.UNKNOWN_TF_OUTPUT_FILE)) {
			return;
		}

		FSDataInputStream in = fileSystem.open(Util.UNKNOWN_TF_OUTPUT_FILE);
		LineReader lineReader = new LineReader(in, context.getConfiguration());
		Text currentLine = new Text("");

		double TFValue = 0;

		while (lineReader.readLine(currentLine) > 0) {
			String[] strings = currentLine.toString().split("\\s+");
			TFValue = Double.parseDouble(strings[1]);
			cache.put(strings[0], TFValue);
		}

		lineReader.close();
		in.close();

	}

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double IDF;
		double TF;
		String keyString = key.toString();

		if (cache.containsKey(keyString)) {
			//get idf value
			String[] strings = values.iterator().next().toString().split("\\|");
			String[] strings2 = strings[1].split(" ");
			IDF = Double.parseDouble(strings2[1]);
			TF = cache.get(key.toString());
			context.write(key, new Text(TF * IDF + ""));
			cache.remove(keyString);
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		long authorCount = Long.parseLong(context.getConfiguration().get("authorCount"));

		for (Entry<String, Double> ent : cache.entrySet()) {
			double TF = ent.getValue();
			double IDF = Math.log10(authorCount); // use 1 for ni (number of sub-collections unigram is in)
			context.write(new Text(ent.getKey()), new Text(TF * IDF + ""));
		}
	}
}