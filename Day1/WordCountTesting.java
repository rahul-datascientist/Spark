package com.jpmc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountTesting{

public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	    private Text word = new Text();
	 
	    public void map(LongWritable key, Text value,  Context context ) throws IOException, InterruptedException  {
	      
	      String line = value.toString();
	    
	      StringTokenizer tokenizer = new StringTokenizer(line);
	      
	      while (tokenizer.hasMoreTokens()) {
	        word.set(tokenizer.nextToken());
	        context.write( word, new IntWritable( 1)  );
	      }
	    
	    }
	  } //end of MyMapper class
	 
	  public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	    public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException {
	      
	      int sum = 0;
	      
	      for (IntWritable val : values) {
				
	          sum += val.get();
	          
	      }
	      
	      context.write( key, new IntWritable(sum) );
	}
	    } //end of MyReducer class

public static void main(String[] args) throws Exception {
		  
		  	Configuration conf = new Configuration();
			
//		    String[] otherArgs = new GenericOptionsParser( conf, args ).getRemainingArgs();
		    
		    Job job = new Job(conf, "Word Counter");
		    
		    job.setJarByClass( WordCountTesting.class );
		    job.setMapperClass( MyMapper.class );
		    job.setReducerClass( MyReducer.class );
		    
		    job.setMapOutputKeyClass( Text.class );
		    job.setMapOutputValueClass( IntWritable.class );
		    
		    job.setOutputKeyClass( Text.class );	    
		    job.setOutputValueClass( IntWritable.class );
		    
		    FileInputFormat.addInputPath( job, new Path( args[0] ) );
		    FileOutputFormat.setOutputPath( job, new Path( args[1] ) );
		    
		    System.exit( job.waitForCompletion( true ) ? 0 : 1 );
	  }
}