
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Activity3{
	
	
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
	
  public static class LocationMapper extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
    	String line = value.toString();
    	if(!line.equals("") && line != null){
    		String [] splitLine = line.split(">");
        	Text index = new Text(splitLine[0].trim() +">");
        	String [] tokens = splitLine[1].replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");;
        	
        	for(int i = 0; i < tokens.length; i++){
        		Text word = new Text(tokens[i].trim());
        		context.write(word, index);
        	} 
    	}	      			
    }
  }

  public static class LemmaReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
    	
      String allIndex = "";
      int count = 0;
      for (Text val : values) {  
        allIndex = allIndex+" "+val;
        count = count + 1;
      }
      Text index = new Text(allIndex + " <count "+Integer.toString(count)+ ">");
      context.write(key, index);
	
	//System.out.println("************Checking in LEMMA FILE***********");      

      if(lemmaMap.containsKey(key.toString())){  
    	  //System.out.println(key);
    	  
    	  List<String> lemmas = lemmaMap.get(key.toString());
    	  //System.out.println(lemmas.get(0));
    	  
    	  if(!lemmas.isEmpty()){
	    	  for(String lemma : lemmas){
	    		  context.write(new Text(lemma), index);
	    	  }
    	  }
      }         
    }
  }

  public static void main(String[] args) throws Exception {
	  
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Lemma");
    job.setJarByClass(Activity3.class);
    job.setMapperClass(LocationMapper.class);
    //job.setCombinerClass(LemmaReducer.class);
    job.setReducerClass(LemmaReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}