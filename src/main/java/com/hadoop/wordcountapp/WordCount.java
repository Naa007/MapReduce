package com.hadoop.wordcountapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {
	public static void main(String[] args) throws Exception {
          //Configuration
                // will load all configurations from core-site.xml 
	         Configuration conf = new Configuration();
	    
		//Old API
	       //Job job = new Job(conf, "wordcount");
		   
		//New API creating a Job by passing conf object
                Job job = Job.getInstance(conf, " wordcount ");
                
                //job starts from this class
	        job.setJarByClass(WordCount.class);
               //give a name to Job 
		job.setJobName("Word Count");
		
               /* As we know we are producing output in key value pair format 
               so producing output Key in Text datatype,Value in IntWritable type */ 
	       job.setOutputKeyClass(Text.class);
	       job.setOutputValueClass(IntWritable.class);
	        
               // Configuring Map and Reduce Classes
	       job.setMapperClass(WordMap.class);
	       job.setReducerClass(WordReduce.class);
	        
               /* setting Input and Output Formats as TextInputFormat, Record Reader reads line by line
               as offset value as key,line as value and gives to Map as an input. */  
	       job.setInputFormatClass(TextInputFormat.class);
	       job.setOutputFormatClass(TextOutputFormat.class);
	        
               // Input files path  and output path (should be inside HDFS)
	       FileInputFormat.addInputPath(job, new Path(args[0]));
	       FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
               // run the job.
	       job.waitForCompletion(true);
	 }
}



