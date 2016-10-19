package cloud;

import org.apache.hadoop.io.Text;

import cloud.TupleC;


public class WordTupleC extends TupleC<Text, Text> {
    public WordTupleC() {
        super(Text.class, Text.class);
    }
    public WordTupleC(Text first, Text second) {
        super(Text.class, Text.class, first, second );
    }
}

