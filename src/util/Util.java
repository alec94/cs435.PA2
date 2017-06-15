package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Util {

	public final static Path AUTHOR_COUNT_OUTPUT_FILE = new Path("/output/authorCount");
	public final static Path OFFLINE_RESULT_OUTPUT_PATH = new Path("/output/offlineResult");
	public final static Path UNKNOWN_UNIGRAM_OUTPUT_PATH = new Path("/output/unknownUnigram");
	public final static Path UNKNOWN_UNIGRAM_OUTPUT_FILE = new Path("/output/unknownUnigram/part-r-00000");
	public final static Path UNKNOWN_TF_OUTPUT_FILE = new Path("/output/unknownTF");
	public final static Path UNKNOWN_TFIDF_OUTPUT_PATH = new Path("/output/unknownTFIDF");
	public final static Path UNKNOWN_TFIDF_OUTPUT_FILE = new Path("/output/unknownTFIDF/part-r-00000");
	public final static Path FINAL_OUTPUT_PATH = new Path("/PA2/result");
	final static Path TF_OUTPUT_PATH = new Path("/output/TF");
	final static Path WORD_COUNT_OUTPUT_PATH = new Path("/output/wordCount");
	final static Path TFIDF_OUTPUT_PATH = new Path("/output/TFIDF");
	final static Path COSINE_OUTPUT_PATH = new Path("/output/Cosine");

	public static void getAuthorCount(Configuration conf) throws IOException {
		Path NumAuthorFile = AUTHOR_COUNT_OUTPUT_FILE;
		FileSystem fs = NumAuthorFile.getFileSystem(conf);

		if (!fs.exists(NumAuthorFile)) {
			return;
		}

		FSDataInputStream in = fs.open(NumAuthorFile);
		long authorCount = in.readLong();
		in.close();
		conf.set("authorCount", authorCount + "");
	}

	static void getMaxUnigramFrequency(Configuration conf) throws IOException {
		FileSystem fileSystem = FileSystem.get(conf);

		if (!fileSystem.exists(UNKNOWN_UNIGRAM_OUTPUT_FILE)) {
			return;
		}

		FSDataInputStream in = fileSystem.open(UNKNOWN_UNIGRAM_OUTPUT_FILE);
		LineReader lineReader = new LineReader(in, conf);
		Text currentLine = new Text("");


		HashMap<String, Integer> table = new HashMap<>();
		int frequency;
		int max = Integer.MIN_VALUE;

		while (lineReader.readLine(currentLine) > 0) {
			String[] strings = currentLine.toString().split("\\s+");
			frequency = Integer.parseInt(strings[1]);
			table.put(strings[0], frequency);

			if (frequency > max) {
				max = frequency;
			}
		}

		lineReader.close();
		in.close();

		//write TF value to output file
		if (fileSystem.exists(UNKNOWN_TF_OUTPUT_FILE)) {
			fileSystem.delete(UNKNOWN_TF_OUTPUT_FILE, true);
		}

		FSDataOutputStream out = fileSystem.create(UNKNOWN_TF_OUTPUT_FILE);

		for (Map.Entry<String, Integer> entry : table.entrySet()) {
			out.writeBytes(entry.getKey() + " " + (double) entry.getValue() / max + "\n");
		}
		out.close();
	}
}
