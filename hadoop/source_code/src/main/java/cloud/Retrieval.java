package cloud;

// import java.io.IOException;
import java.io.*;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Collections;


import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class Retrieval  extends Configured implements Tool {
    public static final int N = 44;

    public static double tfIdf(final double tf, final double df) {
        return tf * Math.log(N / df);
    }

    public static ArrayList<String> Query2list(final String query) {

        ArrayList<String> queryHashTable = new ArrayList<String>();
        for (String term : query.split(" +(AND +|OR +)?-?")) {
            queryHashTable.add(term);
        }
        return queryHashTable;
    }
    
    public static HashMap<String, Integer> parseQuery(final String query) {

        HashMap<String, Integer> queryHashTable = new HashMap<String, Integer>();

        int index = 0;
        for (String term : query.split(" +(AND +|OR +)?-?")) {
            queryHashTable.put(term, index++);
        }

        return queryHashTable;
    }
    

    public static class FilterqueryHashTableper
        extends Mapper<WordIntC, WordInfoList,
        NullWritable, WordDfWordInfoList> {
        private static String query;
        private static HashMap<String, Integer> queryHashTable;

        @Override
        protected final void setup(final Mapper.Context context)
            throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            query = conf.get("query");
            queryHashTable = parseQuery(query);
        }

        public final void map(WordIntC termDf, WordInfoList termInfos
                        , Context context)
            throws IOException, InterruptedException {
            String termString = termDf.getFirst().toString();
            int df = termDf.getSecond().get();
            if (queryHashTable.containsKey(termString))
                context.write(NullWritable.get(), new WordDfWordInfoList( termString, df, termInfos));
        }
    }


    public static class RetrievalReducer extends Reducer<NullWritable, WordDfWordInfoList, NullWritable, DocInfoList> {
        private static String query;
        private static HashMap<String, Integer> queryHashTable ;

        protected void setup(Reducer.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            query = conf.get("query");
            queryHashTable = parseQuery(query);
        }

        public void reduce (NullWritable none, Iterable<WordDfWordInfoList> termDfTermInfos_iter, Context context) throws IOException, InterruptedException {
            ResultHandler resultshandler = new ResultHandler();

            for (WordDfWordInfoList termDfTermInfos: termDfTermInfos_iter ) {
                String term = termDfTermInfos.getTerm();
                int df = termDfTermInfos.getDf();
                ArrayWritable termInfos = termDfTermInfos.getTermInfos();

                ArrayList<DocInfo> fileInfos = new ArrayList<DocInfo>();

                for (Writable entry : termInfos.get()) {
                    WordInfo termInfo = (WordInfo) entry;
                    String fileName = termInfo.getFileName();
                    int tf = termInfo.getTf();
                    LongListW offsets = termInfo.getOffsets();

                    ArrayList<WordPositions> termOffsets = new ArrayList<WordPositions>();
                    termOffsets.add(new WordPositions(term, offsets));
                    fileInfos.add(new DocInfo(fileName, (tfIdf(tf, df)),  termOffsets));
                }

                resultshandler.putFileInfoArray(term, new DocInfoList(fileInfos));
            }
            System.out.println("Start parsing: " + query );
            DocInfoList output = resultshandler.parser.parse(query);
            context.write(NullWritable.get(), output);
        }

    }

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        conf.set("query", args[2]);

        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path(args[0]);
        Path docPath = new Path(args[1]);
        Path parentPath = inputPath.getParent();
        Path outputPath = new Path(parentPath, getClass().getSimpleName());
        fs.delete(outputPath);

        Job job = new Job(getConf(), "Retrieval");

        job.setJarByClass(getClass());


        // input
        SequenceFileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        // map
        job.setMapperClass(FilterqueryHashTableper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(WordDfWordInfoList.class);

        // reducer
        job.setReducerClass(RetrievalReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(DocInfoList.class);

        job.setNumReduceTasks(1);
        // output
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        TextOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);

        Path pt=new Path( outputPath, "part-r-00000");
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, pt, getConf());
        DocInfoList fInfos = new DocInfoList();
        String query = args[2];
        ArrayList<String> qlist = Query2list(query);
        int qsize = qlist.size();
        System.out.println("\n\n-------------------------------------------------\n\nResults");
        while(reader.next(NullWritable.get(),  fInfos)) {
            DocInfo [] fileInfos = (DocInfo [])fInfos.toArray();
            Arrays.sort(fileInfos, Collections.reverseOrder());
            int i = 0;
            for (DocInfo fileInfo : fileInfos) {
                if(qsize == fileInfo.getTermOffsets().size())
                {
                	Boolean hitflag = true;
                	Boolean flag = false;
                	String qterm = qlist.get(0);
                	Long nextoffset;
                	for (WordPositions tmOfs: fileInfo.getTermOffsets())
                	{
                		if(qterm.equals(tmOfs.getTerm()))
                		{
                			for (Long offset: tmOfs.getOffsets())
                			{
                				nextoffset = offset+1+qterm.length();
                				int q = 1;
                				while(q<qsize&&hitflag)
                				{
                					hitflag = false;
                					qterm = qlist.get(q);
                					q++;
                					for (WordPositions tmOfs1: fileInfo.getTermOffsets())
                                	{
                						if(qterm.equals(tmOfs1.getTerm()))
                						{
                							//System.out.println(qterm+" found, now searching for offset "+nextoffset);
                							for (Long offset1: tmOfs1.getOffsets())
                                			{
                                                System.out.println(offset1);
                								if(offset1.equals(nextoffset))
                								{
                									hitflag = true;
                									nextoffset = offset1+1+qterm.length();
                									break;
                								}
                                			}
                						}
                						if(hitflag) 
                							break;
                                	}
                				}
                				if(hitflag)
                            	{
                            		System.out.println("File - "+fileInfo.getFileName()+" Offset - "+offset);
                            	}
                			}
                		}
                	}
                	
                }
                i++;
                }
            }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Retrieval(), args);
        System.exit(exitCode);
    }

}
