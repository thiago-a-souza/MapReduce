package com.thiago;

import java.io.IOException;
import java.util.Iterator;
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

public class MoviesRatedPerUser {
  public static class MoviesRatedPerUserMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",");	
      context.write(new Text(tokens[0]), new IntWritable(1));
    }
  }	
  
  public static class MoviesRatedPerUserReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
    public void reduce(Text key, Iterable<IntWritable> count, Context context) throws IOException, InterruptedException {
      long sum = 0;	
      Iterator<IntWritable> it = count.iterator();

      while(it.hasNext()) {
            it.next();
            sum++;
      }
      context.write(key, new LongWritable(sum));
    }
  }
	
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {	
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "MoviesRatedPerUser");
    job.setJarByClass(MoviesRatedPerUser.class);
	    
    job.setMapperClass(MoviesRatedPerUserMapper.class);	   
    job.setReducerClass(MoviesRatedPerUserReducer.class);
	    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
 	    	    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);		
  }
}