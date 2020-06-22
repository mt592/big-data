

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class YelpAverageStar  {
public static void main(String[] args) throws Exception {
	if (args.length <2) {
	      System.err.println("Usage: WordCount <input path> <output path>");
	      System.exit(-1);
	    }
	//Initializing the map reduce job
	Configuration conf=new Configuration();
	Job job= Job.getInstance(conf, "word count");
	job.setJarByClass(YelpAverageStar.class);
	job.setNumReduceTasks(6);
	
		//Setting the input and output paths.The output file should not already exist. 
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	//Setting the mapper, reducer, and combiner classes
	job.setMapperClass(YelpMapper.class);
	job.setReducerClass(YelpReducer.class);

	//Setting the format of the key-value pair to write in the Map output.
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
	
	//Setting the format of the key-value pair to write in the output file.
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	
	//Submit the job and wait for its completion
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	
}
}
