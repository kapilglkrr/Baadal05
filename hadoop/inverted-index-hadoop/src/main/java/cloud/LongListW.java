package cloud;
import org.apache.hadoop.io.Writable;

import java.util.ArrayList;
import java.util.Collection;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.io.IntWritable;


import cloud.WordInfo;

public class LongListW extends ArrayList<Long> implements Writable {
    public LongListW() {
		super();
		// TODO Auto-generated constructor stub
	}
	public LongListW(Collection<? extends Long> c) {
		super(c);
		// TODO Auto-generated constructor stub
	}
	public LongListW(int initialCapacity) {
		super(initialCapacity);
		// TODO Auto-generated constructor stub
	}
	public void readFields(DataInput in) throws IOException {
        clear();
        int size = in.readInt();
        for(int i = 0; i < size ; i++ ) {
            this.add(in.readLong());
        }
    }
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.size());
        for(long data: this) {
            out.writeLong(data);
        }
    }
}

