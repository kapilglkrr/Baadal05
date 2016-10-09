package cloud;

import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;

import cloud.LongListW;
// import org.apache.hadoop.io.Text;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class WordInfo implements Writable {
    public WordInfo() {
		super();
	}

	public String getFileName() {
		return fileName;
	}

	public WordInfo(String fileName, int tf, LongListW offsets) {
		super();
		this.fileName = fileName;
		this.tf = tf;
		this.offsets = offsets;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public int getTf() {
		return tf;
	}

	public void setTf(int tf) {
		this.tf = tf;
	}

	public LongListW getOffsets() {
		return offsets;
	}

	public void setOffsets(LongListW offsets) {
		this.offsets = offsets;
	}
	private String fileName;
    private int tf;
    private LongListW offsets;

    @Override
    public void readFields(DataInput in) throws IOException {
        Text fnText = new Text();
        fnText.readFields(in);
        fileName = fnText.toString();
        tf = in.readInt();
        // int size = in.readInt();
        // offsets= new ArrayList<Long>(size);
        // for(int i = 0; i < size; i++){
        //   offsets.add(in.readLong());
        // }
        if (offsets == null)
            offsets = new LongListW();
        offsets.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        new Text(fileName).write(out);
        out.writeInt(tf);
        offsets.write(out);
        // out.writeInt(offsets.size());
        // for (long offset : offsets) {
        //   out.writeLong(offset);
        // }
    }
    public String toString() {
        return "(fn=" + this.fileName + ", tf=" + this.tf + ", ofs=" + this.offsets + ")";
    }
}
