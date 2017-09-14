import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

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

public class Activity4_Partb{
	
	static class Lemmatiztion{
			
			public Map<String, List<String>> LemmaFile(){	
		    	
				try{
					Scanner scanner = new Scanner(new File("//home//hadoop//new_lemmatizer.csv"));
					Map<String, List<String>> myMap = new HashMap<String, List<String>>();
			        
			    	while(scanner.hasNext()){
			        	
			        	String line = scanner.next();
			        	String [] allLemmas = line.split(",");
			        	List<String> listOfLemmas = new ArrayList<String>();
			        	
			        	for(int i = 1; i < allLemmas.length; i++){
			        		if(allLemmas[i] != null && !allLemmas[i].equals(""))
			        			listOfLemmas.add(allLemmas[i]);
			        	}
			        	
			        	if(listOfLemmas.isEmpty()){
			        		myMap.put(allLemmas[0], null);
			        	} else{
			        		myMap.put(allLemmas[0], listOfLemmas);
			        	}
			        			
			        }	
			        scanner.close(); 
			        return myMap;
			        
				} catch(Exception e){
					e.printStackTrace();
				}
		    	return null;
		    }
	  }
	
  static Lemmatiztion l = new Lemmatiztion();
  static Map<String, List<String>> lemmaMap = l.LemmaFile();

  public static class PairOccurenceMapper extends Mapper<LongWritable, Text, WordPair, Text>{
	
	private static final WordPair pair = new WordPair();  
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
        String line = value.toString();
    	if(!line.equals("") && line != null){
    		String [] splitLine = line.split(">");
        	Text index = new Text(splitLine[0].trim() +">");
        	String [] tokens = splitLine[1].replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");
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
                        context.write(pair, index);
                        
                        int start_new = (j - neighbour_count < 0) ? 0 : j - neighbour_count;
             	        int end_new = (j + neighbour_count >= tokens.length) ? tokens.length - 1 : j + neighbour_count;
                        
                        for (int k = start; k <= end; k++) {
                            if (k == j) continue;
                            
                            String token_k = tokens[k].toString().toLowerCase();
                            pair.setNextNeighbour(token_k);
                            context.write(pair, index);
                    }
                }
              }
    	}	 
    }
    
  }
  }

  public static class PairCountReducer extends Reducer<WordPair,Text,WordPair,Text> {

    public void reduce(WordPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	
    	String allIndex = "";
        //int count = 0;
        for (Text val : values) {  
          allIndex = allIndex+" "+val;
          //count = count + 1;
        }
        Text index = new Text(allIndex);
        context.write(key, index);
      
      if(lemmaMap.containsKey(key.key.toString())){  
    	  WordPair pair = new WordPair();
    	  List<String> lemmas = lemmaMap.get(key.key.toString());
    	  
    	  if(!lemmas.isEmpty()){
    		  pair.setNeighbour(key.neighbour.toString());
    		  pair.setNextNeighbour(key.nextNeighbour.toString());
    		  
	    	  for(String lemma : lemmas){
	    		  pair.setKey(lemma);
	    		  context.write(pair, index);
	    	  }
    	  }
      }
      
      if(lemmaMap.containsKey(key.neighbour.toString())){  
    	  WordPair pair = new WordPair();
    	  List<String> lemmas = lemmaMap.get(key.neighbour.toString());
    	  
    	  if(!lemmas.isEmpty()){
    		  pair.setKey(key.key.toString());
    		  pair.setNextNeighbour(key.nextNeighbour.toString());
    		  
	    	  for(String lemma : lemmas){
	    		  pair.setNeighbour(lemma);
	    		  context.write(pair, index);
	    	  }
    	  }
      }
      
      if(lemmaMap.containsKey(key.nextNeighbour.toString())){  
    	  WordPair pair = new WordPair();
    	  List<String> lemmas = lemmaMap.get(key.nextNeighbour.toString());
    	  
    	  if(!lemmas.isEmpty()){
    		  pair.setKey(key.key.toString());
    		  pair.setNeighbour(key.neighbour.toString());
    		  
	    	  for(String lemma : lemmas){
	    		  pair.setNextNeighbour(lemma);
	    		  context.write(pair, index);
	    	  }
    	  }
      }     
       
    }
  }
  
  public static class WordPair implements Writable,WritableComparable<WordPair> {

		private Text key;
	    private Text neighbour;
	    private Text nextNeighbour;
	    
	    public WordPair() {
	        this.key = new Text();
	        this.neighbour = new Text();
	        this.nextNeighbour = new Text();
	    }

	    public WordPair(String word, String neighbour, String nextNeighbour) {
	        this(new Text(word),new Text(neighbour), new Text(nextNeighbour));
	    }
	    
	    public WordPair(Text key, Text neighbour, Text nextNeighbour) {
	        this.key = key;
	        this.neighbour = neighbour;
	        this.nextNeighbour = nextNeighbour;
	    }
	    
	    public void setNextNeighbour(String nextNeighbour){
	    	this.nextNeighbour = new Text (nextNeighbour);
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
	    
	    public Text getnextNeighbour() {
	        return nextNeighbour;
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
	               " neighbour=["+neighbour+"]" + " nextNeighbour=["+nextNeighbour+"]}";
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
	        nextNeighbour.write(out);
	    }

	    //@Override
	    public void readFields(DataInput in) throws IOException {
	        key.readFields(in);
	        neighbour.readFields(in);
	        nextNeighbour.readFields(in);
	    }


	}


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Pairs");
    job.setJarByClass(Activity4_Partb.class);
    job.setMapperClass(PairOccurenceMapper.class);
    job.setCombinerClass(PairCountReducer.class);
    job.setReducerClass(PairCountReducer.class);
    job.setOutputKeyClass(WordPair.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}