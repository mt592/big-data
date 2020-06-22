import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class CommonFollowersReducer1 extends
    Reducer<Text, Text, Text, Text> {
	
  public void reduce(Text key, Iterable<Text> values, Context context)
  	
      throws IOException, InterruptedException {
	  
	  ArrayList<String> ORecords = new ArrayList<String>();
	  
	  for (Text value : values) {
		  ORecords.add(value.toString());		  
		  }

	      for (String b : ORecords) {
	    	  for (String a : ORecords) {
	    		  if (Integer.parseInt(b.toString())< Integer.parseInt(a.toString())) {
	    			  String nline = key.toString();
	    			  String[] nKey = nline.split(",");
	    			  
	    			  context.write(new Text (b + " " + a) , new Text(nKey[0]));
	    			  } 
	    		  }
	    	  }
	      } 
  }


		  
		  
		  
	  
	  

