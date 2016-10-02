package com.hadoop.wordcountapp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

 //new API Iterator is replaced by Iterable
   public void reduce(Text key, Iterable<IntWritable> values, Context context) 
     throws IOException, InterruptedException {
       int sum = 0;
       // iterating values to find actual count
       for (IntWritable val : values) {
           sum += val.get();
       }
       // writing output to HDFS
       context.write(key, new IntWritable(sum));
   }
}
