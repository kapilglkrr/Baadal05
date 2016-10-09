package cloud;

import java.lang.reflect.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.String;


public class KeyFrequency  implements WritableComparable {
    private Text key;
    private IntWritable count;

    public KeyFrequency(Text key, IntWritable count) {
		super();
		this.key = key;
		this.count = count;
	}
	public int compareTo(Object object) {
        KeyFrequency ip2 = (KeyFrequency) object;
        int cmp = getKey().compareTo(ip2.getKey());
        if (cmp != 0)
            return cmp;
        return getCount().compareTo(ip2.getCount()); // reverse
    }
    public void readFields(DataInput in) throws IOException {
        if (key == null)
            key = new Text();
        if (count == null)
            count = new IntWritable();

        key.readFields(in);
        count.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        key.write(out);
        count.write(out);
    }

    public String toString() {
        return "(" + this.getKey() + ", " + this.getCount() + ")";
    }
	public Text getKey() {
		return key;
	}
	public void setKey(Text key) {
		this.key = key;
	}
	public IntWritable getCount() {
		return count;
	}
	public void setCount(IntWritable count) {
		this.count = count;
	}
}


