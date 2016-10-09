package cloud;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import cloud.TupleC;

public class WordIntC extends TupleC<Text, IntWritable> {
    public WordIntC() {
        super(Text.class, IntWritable.class);
    }
    public WordIntC(Text first, IntWritable second) {
        super(Text.class, IntWritable.class, first, second );
    }
}

