package cloud;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.ArrayWritable;

import cloud.WordInfo;

public class WordInfoList extends ArrayWritable {


    public WordInfoList() {
        super(WordInfo.class);
    }
    public WordInfoList(WordInfo[] values) {
        super(WordInfo.class, values);
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(getClass().getSimpleName() + "[");
        for (Writable entry : this.get()) {
            WordInfo termInfo = (WordInfo) entry;
            stringBuilder.append(termInfo.toString() + ", ");
        }
        // stringBuilder.append(entry.getKey().toString() + "=" + entry.getValue().toString() + ", ");

        stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length());
        stringBuilder.append("]");

        return stringBuilder.toString();
    }


}
