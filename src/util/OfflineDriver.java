package util;

import AAV.AAVMapper;
import AAV.AAVReducer;
import TF.TFMapper;
import TF.TFReducer;
import TFIDF.TFIDFMapper;
import TFIDF.TFIDFReducer;
import authorCount.AuthorCountMapper;
import authorCount.AuthorCountReducer;
import authorCount.AuthorCountWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class OfflineDriver {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(configuration);

		//get unigrams and TF for known authors
		Job job1 = Job.getInstance(configuration);
		job1.setJarByClass(OfflineDriver.class);
		job1.setMapperClass(TFMapper.class);
		job1.setReducerClass(TFReducer.class);
		job1.setCombinerClass(TFReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, Util.TF_OUTPUT_PATH);

		if (fileSystem.exists(Util.TF_OUTPUT_PATH)) {
			fileSystem.delete(Util.TF_OUTPUT_PATH, true);
		}
		job1.waitForCompletion(true);

		// get author COUNT and word COUNT
		Job job2 = Job.getInstance(configuration);
		job2.setJarByClass(OfflineDriver.class);
		job2.setMapperClass(AuthorCountMapper.class);
		job2.setReducerClass(AuthorCountReducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(AuthorCountWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, Util.TF_OUTPUT_PATH);
		FileOutputFormat.setOutputPath(job2, Util.WORD_COUNT_OUTPUT_PATH);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		if (fileSystem.exists(Util.WORD_COUNT_OUTPUT_PATH)) {
			fileSystem.delete(Util.WORD_COUNT_OUTPUT_PATH, true);
		}
		job2.waitForCompletion(true);

		//get author COUNT
		Counters counters = job2.getCounters();
		Counter c = counters.findCounter(authorCount.COUNT);
		long authorCount = c.getValue();

		if (fileSystem.exists(Util.AUTHOR_COUNT_OUTPUT_FILE)) {
			fileSystem.delete(Util.AUTHOR_COUNT_OUTPUT_FILE, true);
		}

		FSDataOutputStream out = fileSystem.create(Util.AUTHOR_COUNT_OUTPUT_FILE);
		out.writeLong(authorCount);
		out.close();


		// calculate TFIDF for unigrams
		Configuration configuration1 = new Configuration();
		configuration1.set("authorCount", authorCount + "");

		Job job3 = Job.getInstance(configuration1);
		job3.setJarByClass(OfflineDriver.class);
		job3.setMapperClass(TFIDFMapper.class);
		job3.setReducerClass(TFIDFReducer.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job3, Util.WORD_COUNT_OUTPUT_PATH);
		FileOutputFormat.setOutputPath(job3, Util.TFIDF_OUTPUT_PATH);
		job3.setInputFormatClass(KeyValueTextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);

		if (fileSystem.exists(Util.TFIDF_OUTPUT_PATH)) {
			fileSystem.delete(Util.TFIDF_OUTPUT_PATH, true);
		}
		job3.waitForCompletion(true);

		//get AAV for each author
		Job job4 = Job.getInstance(configuration1);
		job4.setJarByClass(OfflineDriver.class);
		job4.setMapperClass(AAVMapper.class);
		job4.setReducerClass(AAVReducer.class);
		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(Text.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job4, Util.TFIDF_OUTPUT_PATH);
		FileOutputFormat.setOutputPath(job4, Util.OFFLINE_RESULT_OUTPUT_PATH);
		job4.setInputFormatClass(KeyValueTextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);

		if (fileSystem.exists(new Path(args[1]))) {
			fileSystem.delete(new Path(args[1]), true);
		}

		job4.waitForCompletion(true);
	}

	public enum authorCount {
		COUNT
	}

}
