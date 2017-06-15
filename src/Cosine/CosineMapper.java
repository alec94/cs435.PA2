package Cosine;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.LineReader;
import util.Util;

import java.io.IOException;
import java.util.HashMap;

public class CosineMapper extends Mapper<Text, Text, Text, DoubleWritable> {

	private HashMap<String, Double> cache = new HashMap<>();
	private double Bi = 0;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		FileSystem fileSystem = FileSystem.get(context.getConfiguration());
		if (!fileSystem.exists(Util.UNKNOWN_TFIDF_OUTPUT_FILE))
			return;
		FSDataInputStream in = fileSystem.open(Util.UNKNOWN_TFIDF_OUTPUT_FILE);
		LineReader lineReader = new LineReader(in, context.getConfiguration());
		Text currentLine = new Text("");

		double TFIDFValue = 0;
		while (lineReader.readLine(currentLine) > 0) {
			String[] strings = currentLine.toString().split("\\s+");
			TFIDFValue = Double.parseDouble(strings[1]);
			cache.put(strings[0], TFIDFValue);
			Bi += TFIDFValue * TFIDFValue;
		}
		lineReader.close();
		in.close();
		Bi = Math.sqrt(Bi);

	}

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String[] wordEntry = value.toString().split(",");
		double Ai = 0;
		double dotProduct = 0;
		double TFIDF;

		for (String entry : wordEntry) {
			String[] strings = entry.split(" ");
			TFIDF = Double.parseDouble(strings[1]);
			if (cache.containsKey(strings[0])) {
				dotProduct += TFIDF * cache.get(strings[0]);
			}
			Ai += TFIDF * TFIDF;
		}
		Ai = Math.sqrt(Ai);
		double similarity = dotProduct / (Ai * Bi);
		context.write(key, new DoubleWritable(similarity));
	}

}
