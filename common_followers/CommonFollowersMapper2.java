
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.*;
import java.util.*;
import java.io.BufferedReader;


public class CommonFollowersMapper2 extends
Mapper<Text, Text, Text, Text> {
	
	  public void map(Text key, Text value, Context context)
	      throws IOException, InterruptedException {
		   
		  String line = value.toString();
		  StringTokenizer record = new StringTokenizer(line, " ");
		  
		  Text key1=new Text();
		  Text key2=new Text();
		  Text v=new Text();
		    
		  while(record.hasMoreTokens()) {
			 key1 = new Text(record.nextToken());
			 key2= new Text (record.nextToken());
			 v= new Text (record.nextToken());
			 
			 context.write(new Text(key1+","+key2), v);
			 }
		  }
	  }

