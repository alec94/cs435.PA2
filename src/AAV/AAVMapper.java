package AAV;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class AAVMapper extends Mapper<Text, Text, Text, Text> {
	private Text outKey = new Text(); //author
	private Text outValue = new Text();

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String[] strings = value.toString().split("\\|");
		outKey.set(strings[0]);
		outValue.set(key + " " + strings[1]);
		context.write(outKey, outValue);
	}
}
