//Author:Shreyas Mohan
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
//import java.util.StringTokenizer;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class BonusNew {
	 static HashMap<String,ArrayList<String>> actorData=new HashMap<String,ArrayList<String>>();
     
  public static class ActorsMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    //actorData contains in following format <actorId,<titleId,actorName>>
   
	private BufferedReader readFile;
    //private Text word = new Text();
    protected void setup(Context context) throws IOException {
    	URI[] cacheFiles = context.getCacheFiles();
    	if (cacheFiles != null && cacheFiles.length > 0)
    	  { 
    	      readFile = new BufferedReader(new FileReader(new File(cacheFiles[0].getPath()).getName()));
    	      //Read the first line of cached file(imdb actors)
    	      String line=readFile.readLine();
    	      while(line!=null) {
    	    	  //Separate words by using ; as delimiter
    	    	  String cols[]=line.split(";");
    	    	  String titleId=cols[0];
    	    	  String actorId=cols[1];
    	    	  String actorName=cols[2];
    	    	 
    	    	  //actorValues will contain [titleId1,actorname1,titleId2,actorname2,..]
    	    	  ArrayList<String> actorValues = new ArrayList<>();
    	    	  ArrayList<String> actorOldValues = new ArrayList<>();
    	    	  
    	    	  //actor id is key,actorValues are values in actorData hash map
    	    	 
    	    	  
    	    	  try {
	    	    	  if(!actorData.isEmpty()) {
	    	    		  if(!actorData.containsKey(actorId)) {
		    	    	  //if(actorData.get(actorId).isEmpty() ) {
		    	    		  actorValues.add(titleId);
		        	    	  actorValues.add(actorName);
		    	    		  actorData.put(actorId, actorValues);
		    	    	  }
		    	    	  else
		    	    	  {
		    	    		actorOldValues=actorData.get(actorId);
		    	    		actorOldValues.add(titleId);
		      	    	    actorOldValues.add(actorName);
		      	    	    actorData.put(actorId, actorOldValues);
		      	    	    
		    	    	  }
	    	    	  }
	    	    	  else {
	    	    		  
	    	    		  actorValues.add(titleId);
	        	    	  actorValues.add(actorName);
	    	    		  actorData.put(actorId, actorValues);
	    	    	  }
    	    	  }
    	    	  catch(NullPointerException e) {
    	    		  System.out.println("Null Exception");
    	    	  }
    	    	 // System.out.println("After"+actorData.get(actorId));
    	    	  //To read next line in cached file 
    	    	  line=readFile.readLine();
    	      }
    	  }
    }
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      //StringTokenizer itr = new StringTokenizer(value.toString());
    	//System.out.println(actorData);
    	//Read line of input file(imdb directors) and split it
    	String arr[] = value.toString().trim().split(";");
    	String titleId=arr[0];
    	String directorId=arr[1];
    	//System.out.println("Reducer "+titleId+" "+directorId);
    	//We are retrieving actor values by comparing if directorId is equal to any of actorId i.e checking if any director has also acted
    	ArrayList<String> actorSet= actorData.get(directorId);
    	//System.out.println(actorSet);
    	try {
    	//Iterating 
    	for(int i=0;i<actorSet.size();i+=2) {
    		//If he/she has acted and directed in the same title
    		if(titleId.equals(actorSet.get(i))) {
    			//Retrieve actor name
    			String new_key=actorSet.get(i+1);
    		//	System.out.println("Actor Name inside loop "+new_key);
    			Text new_key_text = new Text(new_key);
    			//Mapper output-ActorName 1
				context.write(new_key_text, one);
    		}
    	}
    	

        }
    	catch(NullPointerException e) {
    		
    	}
     }
  }
 


  public static class ActorReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      //Counting 
      for (IntWritable val : values) {
        sum += val.get();
      }
      
      if(sum>=100) {
      result.set(sum);
      context.write(key, result);
      }
  }
  }

  public static void main(String[] args) throws Exception {
	
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Bonus");
    job.setJarByClass(BonusNew.class);
    job.setMapperClass(ActorsMapper.class);
    job.setCombinerClass(ActorReducer.class);
    job.setReducerClass(ActorReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    //Pass IMDB_ACTORS as first argument,which gets added into the cache
    job.addCacheFile(new Path(args[0]).toUri());
    //Pass IMDB_DIRECTORS as second argument which is passed as file parameter to mapper 
    FileInputFormat.addInputPath(job, new Path(args[1]));
    //Output File
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  
}

