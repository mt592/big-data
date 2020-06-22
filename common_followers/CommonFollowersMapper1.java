
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.*;
//import org.json.JSONObject;
import java.util.*;
//import org.json.*;
import java.io.BufferedReader;


public class CommonFollowersMapper1 extends
Mapper<LongWritable, Text, Text, Text> {
		
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
		   
		  String line = value.toString();
		  StringTokenizer record = new StringTokenizer(line, " ");
		  
		  Text fol=new Text();
		  Text user=new Text();
		    
		  while(record.hasMoreTokens()) {
			 fol = new Text(record.nextToken());
			 user= new Text (record.nextToken());
			 
			 context.write(new Text(fol), user);
			 }
		  }
	  }



	  