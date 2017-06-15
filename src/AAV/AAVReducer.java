package AAV;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class AAVReducer extends Reducer<Text, Text, Text, Text> {
	//input: <author;   unigram TFIDF IDF>
	//output: <author;   unigram TFIDF IDF, unigram TFIDF IDF, ...>

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		StringBuilder build = new StringBuilder();
		for (Text val : values) {
			build.append(val + ",");
			//context.write(key, val);
		}

		context.write(key, new Text(build.toString()));
	}
}
