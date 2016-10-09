package cloud;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class WordDfWordInfoList implements Writable {
    
	public WordDfWordInfoList() {
		super();
	}

	private String term;
    private int df;
    private WordInfoList termInfos;
    
    public WordDfWordInfoList(String term, int df, WordInfoList termInfos) {
		super();
		this.term = term;
		this.df = df;
		this.termInfos = termInfos;
	}


    @Override
    public void readFields(DataInput in) throws IOException {
        if (termInfos == null)
            termInfos = new WordInfoList();
        Text termText = new Text();
        termText.readFields(in);
        this.term = termText.toString();
        this.df = in.readInt();
        termInfos.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        new Text(term).write(out);
        out.writeInt(df);
        termInfos.write(out);
    }

    @Override
    public String toString() {
        return "<tm=" + this.term + ", df=" + this.df + ", tmInfs=" + termInfos + ">";
    }

	public String getTerm() {
		return term;
	}

	public void setTerm(String term) {
		this.term = term;
	}

	public int getDf() {
		return df;
	}

	public void setDf(int df) {
		this.df = df;
	}

	public WordInfoList getTermInfos() {
		return termInfos;
	}

	public void setTermInfos(WordInfoList termInfos) {
		this.termInfos = termInfos;
	}



}


