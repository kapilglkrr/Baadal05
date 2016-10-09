import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Indexer {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{

    //private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text filename = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
  	{
  		String filenameString = ((FileSplit) context.getInputSplit()).getPath().getName();
  		filename = new Text(filenameString);
  		String line = value.toString();
  		line = line.toLowerCase();
  		StringTokenizer tokenizer = new StringTokenizer(line);
  		while (tokenizer.hasMoreTokens()) {
  			word.set(tokenizer.nextToken());			
  			context.write(word, filename);
  		}
    }
  }

  public static class TokenReducer extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      StringBuilder stringBuilder = new StringBuilder();

    for (Text value : values) {
      stringBuilder.append(value.toString());

      if (values.iterator().hasNext()) {
        stringBuilder.append(" , ");
      }
    }
    result = new Text(stringBuilder.toString());
    context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Indexer.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(TokenReducer.class);
    job.setReducerClass(TokenReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
