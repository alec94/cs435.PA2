package TF;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class TFMapper extends Mapper<Object, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		int start = line.indexOf("<===>");
		if (start == -1) return;
		String author = line.substring(0, start);
		String[] names = author.split(" ");
		author = names[names.length - 1]; // get only authors last name

		String[] unigrams = line.substring(start + 5).replaceAll("[^A-Za-z0-9\\s]", "").toLowerCase().split("\\s+");
		for (String unigram : unigrams) {
			if (!unigram.isEmpty()) {
				context.write(new Text(author + "|" + unigram), one);
			}
		}
	}
}