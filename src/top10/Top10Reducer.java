package top10;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;


/**
 * Created by Alec on 3/25/2017.
 * Gets top 10 TFIDF scores
 */
public class Top10Reducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		ArrayList<String> cache = new ArrayList<>();
		for (Text value : values) {
			if (!value.toString().isEmpty()) {
				//System.out.println("[Reducer] Value: " + value.toString());
				String[] strings = value.toString().replace("\t", " ").split("\\s+");
				//System.out.println("[Reducer] strings: " + Arrays.toString(strings));
				if (strings.length > 1) {
					if (!strings[1].contains("E")) { // throw out really small numbers
						cache.add(strings[1] + " " + strings[0]); // flip order so the TFIDF score is first
					}
				}
			}
		}

		Collections.sort(cache); // sort array
		Collections.reverse(cache);

		String[] top10;
		String[] temp = new String[cache.size()];
		cache.toArray(temp);

		if (temp.length > 10) {
			top10 = new String[10];
			System.arraycopy(temp, 0, top10, 0, 10); // get only top 10
		} else {
			top10 = temp;
		}

		//System.out.println("Top 10: " + Arrays.toString(top10));

		for (String entry : top10) {
			String[] strings = entry.split(" ");
			//System.out.println("OUT: " + strings[0] + "\t" + strings[1]);
			context.write(new Text("Top 10"), new Text(strings[0] + "\t" + strings[1]));
		}

	}
}
