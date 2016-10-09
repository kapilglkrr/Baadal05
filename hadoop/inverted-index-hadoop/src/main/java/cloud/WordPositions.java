package cloud;

import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

import cloud.LongListW;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordPositions implements Writable {
    public WordPositions() {
		super();
	}

	public WordPositions(String term, LongListW offsets) {
		super();
		this.term = term;
		this.offsets = offsets;
	}

	private String term;
     private LongListW offsets;

    @Override
    public void readFields(DataInput in) throws IOException {
        Text tmText = new Text();
        tmText.readFields(in);
        this.term = tmText.toString();
        if (offsets == null)
            offsets = new LongListW();
        offsets.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        new Text(this.term).write(out);
        offsets.write(out);
    }
    
    public String toString() {
        return "(tm=" + this.term + ", ofs=" + this.offsets + ")";
    }
    public boolean equals(Object o)
    {
    	if(o.toString()==this.term)
    		return true;
    	return false;
    }

	public String getTerm() {
		return term;
	}

	public void setTerm(String term) {
		this.term = term;
	}

	public LongListW getOffsets() {
		return offsets;
	}

	public void setOffsets(LongListW offsets) {
		this.offsets = offsets;
	}
}


