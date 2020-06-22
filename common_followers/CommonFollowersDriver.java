import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class CommonFollowersDriver {
public static void main(String[] args) throws Exception {
	if (args.length <2) {
	      System.err.println("Usage: WordCount <input path> <output path>");
	      System.exit(-1);
	    }
	//job1 is the mapreduce job for the first step of matrix multiplication
 	Configuration conf = new Configuration();
	Job job1 = Job.getInstance(conf, "MatrixMultStep1");
	job1.setJarByClass(CommonFollowersDriver.class);  
    
	//Create a temporary file to store the result of job1
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    Path tempOut = new Path("temp");
    SequenceFileOutputFormat.setOutputPath(job1, tempOut);
    job1.setOutputFormatClass(SequenceFileOutputFormat.class);

    job1.setMapperClass(CommonFollowersMapper1.class);
    job1.setReducerClass(CommonFollowersReducer1.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    job1.waitForCompletion(true);

    //Job2 is the mapreduce job for the second step of matrix multiplication
    Job job2 = Job.getInstance(conf, "MatrixMultStep1");
	job2.setJarByClass(CommonFollowersDriver.class);  

    //The input of job2 is the output of job 1
    job2.setInputFormatClass(SequenceFileInputFormat.class);
    SequenceFileInputFormat.addInputPath(job2, tempOut);
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    job2.setReducerClass(CommonFollowersReducer2.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(Text.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    job2.waitForCompletion(true);
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
}
}
