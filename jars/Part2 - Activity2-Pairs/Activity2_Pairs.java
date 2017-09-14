import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/* Word co-occurrence on tweets:
 * 
First step in sentiment analysis is the co-occurrence of the topic of interests with 
words representing good or bad sentiments. We will not perform sentiment analysis. We 
will do just word co-occurrence. Perform word co-occurrence as described in Lin and 
Dyer’s text [3], with pairs and stripes methods for the tweets collected for the step 
above. Of course, use MapReduce approach with the data stored on the HDFS of the VM you 
installed. 

Input: Tweets 
Output: Co-occurrence Pairs
Processing: MR on HDFS
 * 
 * */

public class Activity2_Pairs{

  public static class PairOccurenceMapper extends Mapper<LongWritable, Text, WordPair, IntWritable>{
	
	private static final WordPair pair = new Activity2_Pairs.WordPair();  
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    	
        String[] tokens = value.toString().split("\\s+");
        int neighbour_count = context.getConfiguration().getInt("neighbours", tokens.length);
        
        if (tokens.length > 1) {
          for (int i = 0; i < tokens.length; i++) {
        	  String token_i = tokens[i].toString().toLowerCase();
        	  pair.setKey(token_i);
             
        	  int start = (i - neighbour_count < 0) ? 0 : i - neighbour_count;
 	          int end = (i + neighbour_count >= tokens.length) ? tokens.length - 1 : i + neighbour_count;
             
             for (int j = start; j <= end; j++) {
                  if (j == i) continue;
                  
                  String token_j = tokens[j].toString().toLowerCase();
                  pair.setNeighbour(token_j);
                  context.write(pair, one);
              }
          }
         }
    }
    
  }

  public static class PairCountReducer extends Reducer<WordPair,IntWritable,WordPair,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    	
      int count = 0;
      
      for (IntWritable val : values) {
        count += val.get();
      }
      result.set(count);
      context.write(key, result);
    }
  }
  
  public static class WordPair implements Writable,WritableComparable<WordPair> {

		private Text key;
	    private Text neighbour;
	    
	    public WordPair() {
	        this.key = new Text();
	        this.neighbour = new Text();
	    }

	    public WordPair(String word, String neighbour) {
	        this(new Text(word),new Text(neighbour));
	    }
	    
	    public WordPair(Text key, Text neighbour) {
	        this.key = key;
	        this.neighbour = neighbour;
	    }
	    
	    public void setNeighbour(String neighbour){
	    	this.neighbour = new Text (neighbour);
	    }

		public void setKey(String key) {
			// TODO Auto-generated method stub
			this.key = new Text(key);
		}
		
		public Text getKey() {
	        return key;
	    }

	    public Text getneighbour() {
	        return neighbour;
	    }
		
		//@Override
	    public int compareTo(WordPair other) {
	        int returnVal = this.key.compareTo(other.getKey());
	        if(returnVal != 0){
	            return returnVal;
	        }
	        if(this.neighbour.toString().equals("*")){
	            return -1;
	        }else if(other.getneighbour().toString().equals("*")){
	            return 1;
	        }
	        return this.neighbour.compareTo(other.getneighbour());
	    }

	    @Override
	    public String toString() {
	        return "{word=["+key+"]"+
	               " neighbour=["+neighbour+"]}";
	    }
		
		public static WordPair read(DataInput in) throws IOException {
	        WordPair wordPair = new WordPair();
	        wordPair.readFields(in);
	        return wordPair;
	    }

	    //@Override
	    public void write(DataOutput out) throws IOException {
	        key.write(out);
	        neighbour.write(out);
	    }

	    //@Override
	    public void readFields(DataInput in) throws IOException {
	        key.readFields(in);
	        neighbour.readFields(in);
	    }


	}


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Pairs");
    job.setJarByClass(Activity2_Pairs.class);
    job.setMapperClass(PairOccurenceMapper.class);
    job.setCombinerClass(PairCountReducer.class);
    job.setReducerClass(PairCountReducer.class);
    job.setOutputKeyClass(Activity2_Pairs.WordPair.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}