import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class YelpReducer extends
    Reducer<Text, IntWritable, Text, Text> {
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    int sum = 0;
    int count=0;
    //Summing up the counts for each word
    for (IntWritable value : values) {
        //String[] tokens = value.toString().split(",");
        count++;
      sum += value.get();
    }
    
    double average= Math.round(((double) sum/ (double) count)*100.0)/100.0;

    context.write(new Text("business_id: " +key), new Text(" average_stars: " + Double.toString(average) +
    		" review_count: "+ Integer.toString(count)));
  }
}

