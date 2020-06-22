
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class CommonFollowersReducer2 extends
    Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  List<String> followers=new ArrayList<String>();
	  for(Text t:values) {
		  followers.add(t.toString());  
	  }
	  context.write(key, new Text(followers.toString())); 
  	}
		  
}
	  
