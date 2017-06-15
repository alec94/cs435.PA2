package authorCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class AuthorCountMapper extends Mapper<Text, Text, Text, AuthorCountWritable> {
	AuthorCountWritable output = new AuthorCountWritable();

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String[] authorUnigram = key.toString().split("\\|");
		output.setFrequency(value);
		output.setUnigram(authorUnigram[1]);
		context.write(new Text(authorUnigram[0]), output);
	}
}