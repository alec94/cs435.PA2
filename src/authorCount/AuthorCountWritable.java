package authorCount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AuthorCountWritable implements Writable {
	private Text unigram;
	private Text frequency;

	//have to use default constructor because MR is stupid
	AuthorCountWritable() {
		unigram = new Text();
		frequency = new Text();
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		unigram.readFields(arg0);
		frequency.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		unigram.write(arg0);
		frequency.write(arg0);
	}

	public void setUnigram(Text uni) {
		unigram = uni;
	}

	public Text getUnigram() {
		return unigram;
	}

	public void setUnigram(String uni) {
		unigram.set(uni);
	}

	int getFrequency() {
		return Integer.parseInt(frequency.toString());
	}

	void setFrequency(Text f) {
		frequency = f;
	}
	//public Text getFrequencyText() {return frequency;}


}
