package cloud;

import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

import cloud.LongListW;
import cloud.WordPositions;
// import org.apache.hadoop.io.Text;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DocInfo implements WritableComparable {
    public DocInfo() {
		super();
	}

	public DocInfo(String fileName, double score,
			ArrayList<WordPositions> wordOffsers) {
		super();
		this.fileName = fileName;
		this.score = score;
		this.wordOffsers = wordOffsers;
	}

	private String fileName;
    private double score;
    private ArrayList<WordPositions> wordOffsers;

    @Override
    public void readFields(DataInput in) throws IOException {
        Text fnText = new Text();
        fnText.readFields(in);
        fileName = fnText.toString();
        score = in.readDouble();
        if(wordOffsers == null)
            wordOffsers = new ArrayList<WordPositions>();
        else
            wordOffsers.clear();
        int size = in.readInt();
        for(int i = 0; i < size ; i++ ) {
            WordPositions tfs = new WordPositions();
            tfs.readFields(in);
            wordOffsers.add(tfs);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        new Text(fileName).write(out);
        out.writeDouble(score);
        // wordOffsers.write(out);
        out.writeInt(wordOffsers.size());
        for(WordPositions tfs: wordOffsers)
            tfs.write(out);
    }

    public int compareTo(Object object) {
        DocInfo other = (DocInfo) object;
        if (this.score > other.score)
            return 1;
        else if (this.score == other.score)
            return 0;
        else return -1;
    }
    public String toString() {
        return "(fn=" + this.fileName + ", score=" + this.score + ", tm_ofs=" + this.wordOffsers + ")";
    }

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public ArrayList<WordPositions> getTermOffsets() {
		return wordOffsers;
	}

	public void settermOffsets(ArrayList<WordPositions> wordOffsers) {
		this.wordOffsers = wordOffsers;
	}

}


