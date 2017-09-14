
import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
Output: Co-occurrence Stripes
Processing: MR on HDFS
 * */

public class Activity2_Stripes{

  public static class StripesOccurenceMapper extends Mapper<LongWritable,Text,Text, myMapWritable>{

    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       myMapWritable occurrenceMap = new Activity2_Stripes.myMapWritable();
    	
 	   String[] tokens = value.toString().split("\\s+");
 	   int neighbour_count = context.getConfiguration().getInt("neighbours", tokens.length);
 	   	
 	   if (tokens.length > 1) {
 	      for (int i = 0; i < tokens.length; i++) {
 	    	  
 	    	  String token_i = tokens[i].toLowerCase();
 	          word.set(token_i);
 	          occurrenceMap.clear();

 	          int start = (i - neighbour_count < 0) ? 0 : i - neighbour_count;
 	          int end = (i + neighbour_count >= tokens.length) ? tokens.length - 1 : i + neighbour_count;  

 	           for (int j = start; j <= end; j++) {
 	                if (j == i) continue;
 	                Text neighbour = new Text(tokens[j]);
 	                if(occurrenceMap.containsKey(neighbour)){
 	                   IntWritable count = (IntWritable)occurrenceMap.get(neighbour);
 	                   count.set(count.get()+1);
 	                }else{
 	                   occurrenceMap.put(neighbour,new IntWritable(1));
 	                }
 	           }
 	          context.write(word,occurrenceMap);
 	       }
 	   }
    }
  }
  

  public static class StripesCountReducer extends Reducer<Text,MapWritable,Text,myMapWritable> {
	 private myMapWritable increaseMap = new Activity2_Stripes.myMapWritable();

    public void reduce(Text key, Iterable<myMapWritable> values, Context context) throws IOException, InterruptedException {
    	myMapWritable increaseMap = new Activity2_Stripes.myMapWritable();
        increaseMap.clear();
        
        for (myMapWritable val : values) {
        	addAll(val);
        }
        context.write(key, increaseMap);
    }
    
    private void addAll(myMapWritable mapWritable) {
        Set<Writable> keys = mapWritable.keySet();
        
        for (Writable key : keys) {
            IntWritable fromCount = (IntWritable) mapWritable.get(key);
            
            if (increaseMap.containsKey(key)) {
                IntWritable count = (IntWritable) increaseMap.get(key);
                count.set(count.get() + fromCount.get());
            } else {
            	increaseMap.put(key, fromCount);
            	
            }
        }
    }
  } 
  
  private static class myMapWritable extends MapWritable{
	    @Override
	    public String toString(){
	       String s = new String("{ ");
	       Set<Writable> keys = this.keySet();
	       for(Writable key : keys){
	          IntWritable count = (IntWritable) this.get(key);
	          s =  s + key.toString() + "=" + count.toString() + ",";
	       }

	       s = s + " }";
	       return s;
	    }
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Stripes");
    job.setJarByClass(Activity2_Stripes.class);
    job.setMapperClass(StripesOccurenceMapper.class);
    job.setCombinerClass(StripesCountReducer.class);
    job.setReducerClass(StripesCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Activity2_Stripes.myMapWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
