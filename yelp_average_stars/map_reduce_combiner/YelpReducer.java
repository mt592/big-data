import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class YelpReducer extends
    Reducer<Text, Text, Text, Text> {
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    int sum = 0;
    int count=0;
    //Summing up the counts for each word
    
    //Iterator<Text> iterator=values.iterator();
    for (Text value : values) {
   
        String[] tokens = value.toString().split(",");
        
        int temp_count=Integer.parseInt(tokens[1].trim());
        int temp_sum=Integer.parseInt(tokens[0].trim());
        
  
        count+=temp_count;
        sum+=temp_sum;
    }
    
    
    double average= Math.round(((double) sum/ (double) count)*100.0)/100.0;
    
    context.write(new Text("business_id: " +key), new Text(" average_stars: " + Double.toString(average) +
    		" review_count: "+ Integer.toString(count)));
  }
}

