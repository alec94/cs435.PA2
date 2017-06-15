package util;

import Cosine.CosineMapper;
import Cosine.CosineReducer;
import combine.CombineMapper;
import combine.CombineReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import top10.Top10Mapper;
import top10.Top10Reducer;
import unigram.UnigramMapper;
import unigram.UnigramReducer;

public class AuthorDetectDriver {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(configuration);

		// get unigrams for unknown author
		Job job1 = Job.getInstance(configuration);
		job1.setJarByClass(OfflineDriver.class);
		job1.setMapperClass(UnigramMapper.class);
		job1.setCombinerClass(UnigramReducer.class);
		job1.setReducerClass(UnigramReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, Util.UNKNOWN_UNIGRAM_OUTPUT_PATH);

		if (fileSystem.exists(Util.UNKNOWN_UNIGRAM_OUTPUT_PATH)) {
			fileSystem.delete(Util.UNKNOWN_UNIGRAM_OUTPUT_PATH, true);
		}

		job1.waitForCompletion(true);

		Util.getMaxUnigramFrequency(configuration);
		Util.getAuthorCount(configuration);

		//combine unknown author with known authors
		Job job2 = Job.getInstance(configuration);
		job2.setJarByClass(OfflineDriver.class);
		job2.setMapperClass(CombineMapper.class);
		job2.setReducerClass(CombineReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, Util.TFIDF_OUTPUT_PATH);
		FileOutputFormat.setOutputPath(job2, Util.UNKNOWN_TFIDF_OUTPUT_PATH);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		if (fileSystem.exists(Util.UNKNOWN_TFIDF_OUTPUT_PATH)) {
			fileSystem.delete(Util.UNKNOWN_TFIDF_OUTPUT_PATH, true);
		}

		job2.waitForCompletion(true);

		// calculate similarity to known authors
		Job job3 = Job.getInstance(configuration);
		job3.setJarByClass(OfflineDriver.class);
		job3.setMapperClass(CosineMapper.class);
		job3.setReducerClass(CosineReducer.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job3, Util.OFFLINE_RESULT_OUTPUT_PATH);
		FileOutputFormat.setOutputPath(job3, Util.COSINE_OUTPUT_PATH);
		job3.setInputFormatClass(KeyValueTextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);

		if (fileSystem.exists(Util.COSINE_OUTPUT_PATH)) {
			fileSystem.delete(Util.COSINE_OUTPUT_PATH, true);
		}

		job3.waitForCompletion(true);

		// get top 10 matches
		Job job4 = Job.getInstance(configuration);
		job4.setJarByClass(OfflineDriver.class);
		job4.setMapperClass(Top10Mapper.class);
		job4.setReducerClass(Top10Reducer.class);
		job4.setCombinerClass(Top10Reducer.class); // can use reducer for combiner
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job4, Util.COSINE_OUTPUT_PATH);
		FileOutputFormat.setOutputPath(job4, Util.FINAL_OUTPUT_PATH);
		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);

		if (fileSystem.exists(Util.FINAL_OUTPUT_PATH)) {
			fileSystem.delete(Util.FINAL_OUTPUT_PATH, true);
		}

		job4.waitForCompletion(true);
	}
}
