package cloud;

import java.io.IOException;
import java.util.*;
import java.util.regex.MatchResult;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.cli.*;

import cloud.LongListW;
import cloud.WordInfo;
import cloud.WordIntC;
import cloud.WordTupleC;
import cloud.WordInfoList;

import com.google.common.collect.Iterables;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class BuildInvertedIndex  extends Configured implements Tool {

	public static String[] stopWords = {
		"<doc ","</doc>","without", "see", "unless", "due", "also", "must", "might", "like", "]", "[", "}", "{", "<", ">", "?", "\"", "\\", "/", ")", "(", "will", "may", "can", "much", "every", "the", "in", "other", "this", "the", "many", "any", "an", "or", "for", "in", "an", "an ", "is", "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren’t", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can’t", "cannot", "could",
		"couldn’t", "did", "didn’t", "do", "does", "doesn’t", "doing", "don’t", "down", "during", "each", "few", "for", "from", "further", "had", "hadn’t", "has", "hasn’t", "have", "haven’t", "having",
		"he", "he’d", "he’ll", "he’s", "her", "here", "here’s", "hers", "herself", "him", "himself", "his", "how", "how’s", "i ", " i", "i’d", "i’ll", "i’m", "i’ve", "if", "in", "into", "is",
		"isn’t", "it", "it’s", "its", "itself", "let’s", "me", "more", "most", "mustn’t", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "ought", "our", "ours", "ourselves",
		"out", "over", "own", "same", "shan’t", "she", "she’d", "she’ll", "she’s", "should", "shouldn’t", "so", "some", "such", "than",
		"that", "that’s", "their", "theirs", "them", "themselves", "then", "there", "there’s", "these", "they", "they’d", "they’ll", "they’re", "they’ve",
		"this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn’t", "we", "we’d", "we’ll", "we’re", "we’ve",
		"were", "weren’t", "what", "what’s", "when", "when’s", "where", "where’s", "which", "while", "who", "who’s", "whom",
		"why", "why’s", "with", "won’t", "would", "wouldn’t", "you", "you’d", "you’ll", "you’re", "you’ve", "your", "yours", "yourself", "yourselves",
		"Without", "See", "Unless", "Due", "Also", "Must", "Might", "Like", "Will", "May", "Can", "Much", "Every", "The", "In", "Other", "This", "The", "Many", "Any", "An", "Or", "For", "In", "An", "An ", "Is", "A", "About", "Above", "After", "Again", "Against", "All", "Am", "An", "And", "Any", "Are", "Aren’t", "As", "At", "Be", "Because", "Been", "Before", "Being", "Below", "Between", "Both", "But", "By", "Can’t", "Cannot", "Could",
		"Couldn’t", "Did", "Didn’t", "Do", "Does", "Doesn’t", "Doing", "Don’t", "Down", "During", "Each", "Few", "For", "From", "Further", "Had", "Hadn’t", "Has", "Hasn’t", "Have", "Haven’t", "Having",
		"He", "He’d", "He’ll", "He’s", "Her", "Here", "Here’s", "Hers", "Herself", "Him", "Himself", "His", "How", "How’s", "I ", " I", "I’d", "I’ll", "I’m", "I’ve", "If", "In", "Into", "Is",
		"Isn’t", "It", "It’s", "Its", "Itself", "Let’s", "Me", "More", "Most", "Mustn’t", "My", "Myself", "No", "Nor", "Not", "Of", "Off", "On", "Once", "Only", "Ought", "Our", "Ours", "Ourselves",
		"Out", "Over", "Own", "Same", "Shan’t", "She", "She’d", "She’ll", "She’s", "Should", "Shouldn’t", "So", "Some", "Such", "Than",
		"That", "That’s", "Their", "Theirs", "Them", "Themselves", "Then", "There", "There’s", "These", "They", "They’d", "They’ll", "They’re", "They’ve",
		"This", "Those", "Through", "To", "Too", "Under", "Until", "Up", "Very", "Was", "Wasn’t", "We", "We’d", "We’ll", "We’re", "We’ve",
		"Were", "Weren’t", "What", "What’s", "When", "When’s", "Where", "Where’s", "Which", "While", "Who", "Who’s", "Whom",
		"Why", "Why’s", "With", "Won’t", "Would", "Wouldn’t", "You", "You’d", "You’ll", "You’re", "You’ve", "Your", "Yours", "Yourself", "Yourselves"
		};
	public static ArrayList<String> stopWordsList = new ArrayList<String>(Arrays.asList(stopWords));
    public static class IndexMapper extends Mapper<LongWritable, Text, WordTupleC, WordInfo> {
        private Text word = new Text();

        String fName;
        Scanner sc;
        protected void setup(Context context) throws IOException, InterruptedException {
            fName = ((FileSplit) context.getInputSplit()).getPath().getName().toString();
        }

        public void map(LongWritable position, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            sc = new Scanner(line).useDelimiter("[^a-zA-Z]");
            while (sc.hasNext()) {
            	Text wordText = new Text(sc.next());
            	if (stopWordsList.contains(wordText.toString()))
            		continue;
                MatchResult match = sc.match();
                LongListW pos_in_file = new LongListW();
                pos_in_file.add(match.start() + position.get());	//offset of word in file
                context.write(new WordTupleC(wordText, new Text(fName)), new WordInfo(fName, 1, pos_in_file));
            }

        }
    }
    
    public static class IndexCombiner extends Reducer<WordTupleC, WordInfo, WordTupleC, WordInfo> {

        public void reduce (WordTupleC termFile, Iterable<WordInfo> wordInfos, Context context)
            throws IOException, InterruptedException {
            int sum = 0;
            LongListW pos_in_file = new LongListW();


            for (WordInfo termInfo: wordInfos) {
                sum += termInfo.getTf();
                pos_in_file.addAll(termInfo.getOffsets());
            }
            Collections.sort(pos_in_file);

            context.write(termFile, new WordInfo ( termFile.getSecond().toString(), sum, pos_in_file ));
        }
    }


    public static class IndexPartitioner extends Partitioner<WordTupleC, WordInfo> {
        @Override
        public int getPartition(WordTupleC termFile, WordInfo termInfo, int numPartitions) {
            return ( termFile.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(WordTupleC.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            return ((WordTupleC)w1).getFirst ().compareTo (((WordTupleC)w2).getFirst ());
        }
    }

    public static class IndexReducer extends Reducer<WordTupleC, WordInfo, WordIntC, WordInfoList> {
        public void reduce (WordTupleC termFile, Iterable<WordInfo> wordInfos, Context context)
            throws IOException, InterruptedException {

            ArrayList<WordInfo> wordInfos_lst = new ArrayList<WordInfo>();
            for (WordInfo termInfo: wordInfos)
                wordInfos_lst.add((WordInfo)WritableUtils.clone(termInfo, context.getConfiguration()));

            WordInfo [] wordInfos_Array = wordInfos_lst.toArray(new WordInfo[wordInfos_lst.size()]);
            // TermInfo [] wordInfos_Array = Iterables.toArray(wordInfos, TermInfo.class);
            IntWritable df = new IntWritable(wordInfos_Array.length);

            context.write(new WordIntC(termFile.getFirst(), df), new WordInfoList(wordInfos_Array));

        }
    }

    private Job countTfJob(Path inputPath, Path outputPath, Boolean doTextOutput) throws IOException {

        Job job = new Job(getConf(), "Indexing");
        job.setJarByClass(getClass());
        TextInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(IndexMapper.class);
        job.setMapOutputKeyClass(WordTupleC.class);
        job.setMapOutputValueClass(WordInfo.class);
        job.setCombinerClass(IndexCombiner.class);
        job.setPartitionerClass(IndexPartitioner.class);
        job.setGroupingComparatorClass (GroupComparator.class);
        job.setReducerClass(IndexReducer.class);
        job.setOutputKeyClass(WordIntC.class);
        job.setOutputValueClass(WordInfoList.class);
        // output
        if (doTextOutput == true) {
            TextOutputFormat.setOutputPath(job, outputPath);
            job.setOutputFormatClass(TextOutputFormat.class);
        } else {
            SequenceFileOutputFormat.setOutputPath(job, outputPath);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
        }

        return job;
    }

    public void printHelp(Options options){
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "BuildInvertedIndex [OPTION]... <INPUTPATH> <OUTPUTPATH>", "Process text files in INPUTPATH and build inverted index to OUTPUTPATH.\n", options, "");

    }

    @Override
    public int run(String[] args) throws Exception {

        FileSystem fs = FileSystem.get(getConf());
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        fs.delete(outputPath);

        Boolean doTextOutput = false;

        return (countTfJob(inputPath, outputPath, doTextOutput).waitForCompletion(true) ? 1: 0);
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BuildInvertedIndex(), args);
        System.exit(exitCode);
    }

}
