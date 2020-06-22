import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.*;
import org.json.JSONObject;
import java.util.*;
import org.json.*;


public class YelpCombiner extends
	Reducer<Text, Text, Text, Text> {
	  public void reduce(Text key, Iterable<Text> partial_values, Context context)
	      throws IOException, InterruptedException {
  
		  int partial_sum = 0;
		  int partial_count=0;
		    //Summing up the counts for each word
		  
		  for (Text partial_value : partial_values) {
		        //String[] tokens = value.toString().split(",");
			  partial_count++;
		      partial_sum+=Integer.parseInt(partial_value.toString());
		    }
		 
		    context.write(key, new Text(Integer.toString(partial_sum) + ","+ Integer.toString(partial_count)));
		    //context.write(key, new Text(Integer.toString(partial_sum)));

  }
}