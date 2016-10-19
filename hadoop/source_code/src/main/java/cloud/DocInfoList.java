package cloud;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.HashSet;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
// import org.apache.hadoop.io.IntWritable;

import cloud.WordInfo;

public class DocInfoList extends ArrayWritable {
    private HashSet<String> notFileNameSet = new HashSet<String>();
    private boolean notFlag= false;

    public DocInfoList() {
        super(DocInfo.class);
    }
    public DocInfoList(DocInfo[] values) {
        super(DocInfo.class, values);
    }

    public DocInfoList (ArrayList<DocInfo> values) {
        super(DocInfo.class);
        DocInfo [] values_A = values.toArray(new DocInfo[values.size()]);
        this.set(values_A);
    }
    private DocInfoList filterBySet(DocInfoList data, DocInfoList filter) {
        DocInfo [] fileInfos = (DocInfo [])data.get();
        ArrayList<DocInfo> result = new ArrayList<DocInfo>();
        for (DocInfo fileInfo: fileInfos)
            if( ! filter.notFileNameSet.contains(fileInfo.getFileName()))
                result.add(fileInfo);
        DocInfo [] result_A = result.toArray(new DocInfo[result.size()]);
        return new DocInfoList(result_A);

    }
    public DocInfoList and(DocInfoList other) {
        if (this.notFlag && other.notFlag ) {
            this.notFileNameSet.addAll(other.notFileNameSet);
            return this;
        } else if (this.notFlag ) {
            return filterBySet(other, this);
        } else if(other.notFlag) {
            return filterBySet(this, other);
        }

        Writable [] ours = this.get();
        Writable [] theirs = other.get();
        ArrayList<DocInfo> result = new ArrayList<DocInfo>();

        int oi = 0;
        int ti = 0;
        while(oi < ours.length && ti < theirs.length) {
            DocInfo our = (DocInfo)ours[oi];
            DocInfo their = (DocInfo)theirs[ti];
            int res = our.getFileName().compareTo(their.getFileName());
            if ( res == 0) {
                double score = our.getScore() + their.getScore();
                our.getTermOffsets().addAll(their.getTermOffsets());
                result.add(new DocInfo(their.getFileName(), score, our.getTermOffsets()));

                ti++;
                oi++;
            } else if ( res > 0 ) ti++;
            else oi++;
        }

        DocInfo [] result_A = result.toArray(new DocInfo[result.size()]);
        return new DocInfoList(result_A);
    }

    public DocInfoList or(DocInfoList other) {
        DocInfo [] ours = (DocInfo [])this.get();
        DocInfo [] theirs = (DocInfo [])other.get();
        ArrayList<DocInfo> result = new ArrayList<DocInfo>();

        int oi = 0;
        int ti = 0;
        while(oi < ours.length && ti < theirs.length) {
            DocInfo our = ours[oi];
            DocInfo their = theirs[ti];
            int res = our.getFileName().compareTo(their.getFileName());

            if (res == 0) {
                double score = (our.getScore() > their.getScore())? our.getScore(): their.getScore();
                our.getTermOffsets().addAll(their.getTermOffsets());
                result.add(new DocInfo(their.getFileName(), score, our.getTermOffsets()));
                ti++;
                oi++;
            } else {
                result.add(our);
                result.add(their);
                ti++;
                oi++;
            }

        }

        if (oi < ours.length)
            result.addAll(Arrays.asList(Arrays.copyOfRange(ours, oi ,ours.length)));
        if (ti < theirs.length)
            result.addAll(Arrays.asList(Arrays.copyOfRange(theirs, ti ,theirs.length)));

        // System.out.println("length of OR result: " + result.size());
        DocInfo [] result_A = result.toArray(new DocInfo[result.size()]);
        return new DocInfoList(result_A);
    }

    public DocInfoList not() {

        // System.out.println("#1");
        DocInfo [] fileInfos = (DocInfo []) this.get();
        // System.out.println("#2");
        if(notFileNameSet == null) {
            // System.out.println("#3");
            notFileNameSet = new HashSet<String>();
        }
        // System.out.println("#4");


        for (DocInfo fileInfo: fileInfos) {
            // System.out.println("#5");
            notFileNameSet.add(fileInfo.getFileName());
        }

        // System.out.println("#6");
        notFlag = true;
        // this.set(new FileInfo [0]);
        return this;
    }


    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        if (notFlag) {
            stringBuilder.append("-");
            stringBuilder.append(getClass().getSimpleName() + "[");
            for(String fn: notFileNameSet){
                stringBuilder.append(fn + ", ");
            }

        }
        else {

            stringBuilder.append(getClass().getSimpleName() + "[");

            if (this.get().length == 0) {
                stringBuilder.append("]");
                return stringBuilder.toString();
            }
            

            for (Writable entry : this.get()) {
                DocInfo fileInfo = (DocInfo) entry;
                // System.out.println("entry: " + fileInfo);
                stringBuilder.append(fileInfo.getFileName() + ", ");
            }
            // stringBuilder.append(entry.getKey().toString() + "=" + entry.getValue().toString() + ", ");
            
        }

        stringBuilder.delete(stringBuilder.length() - 2, stringBuilder.length());
        stringBuilder.append("]");

        return stringBuilder.toString();
    }


}
