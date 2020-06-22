import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.*;
import org.json.JSONObject;
import java.util.*;
import org.json.*;


public class YelpMapper extends
Mapper<LongWritable, Text, Text, IntWritable> {
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
   ///Replacing all digits and punctuation with an empty string
	  //String line = value.toString().replaceAll("\\p{Punct}|\\d", "").toLowerCase();
   //Extracting the words
	  //StringTokenizer record = new StringTokenizer(line);

	   //Emitting each word as a key and one as its value
	  //while (record.hasMoreTokens()) {
		  
		  JSONObject jsn=new JSONObject(value.toString());
		  
		  //for (int i=0; i<2 ; i++) {
			  //String business_id = (String) jsn.names().getString(i);
			  String business_id= (String) jsn.getString("business_id");
			  int stars=(Integer) jsn.getInt("stars");
			  //int numRev = 1;
			  context.write(new Text(business_id), new IntWritable((int) stars));
		  //} 
		  
  }
}
	  ///////////////////////////
	  //This works to produce total row counts
	  //JSONObject jsn=new JSONObject(value.toString());
	  
	  //for (int i=0; i<jsn.names().length() ; i++) {
		//  String business_id = (String) jsn.names().getString(i);
		//  //int stars=(Integer) jsn.get("stars");		 
		//  context.write(new Text(business_id), new IntWritable(1));
	  //}
		//////////////////////////
	  
	  
//JSONObject jsn=new JSONObject(value.toString());
//int stars = (Integer) jsn.get("stars");	
//String business_id = (String) jsn.get("business_id");	
//context.write(new Text(business_id), new IntWritable(2));
	  
	  
	  /////WORKSSS to blank output
	  //while(keys.hasNext()) {
		  
		  //String keyid=keys.next();	
		  
		  //if(jsn.get(keyid) instanceof JSONObject) {
			 // String business_id = (String) jsn.get("business_id");
			 // int stars = (Integer) jsn.get("stars");
			  //System.out.println(stars);
			 // context.write(new Text(business_id + "," + stars), new IntWritable(1));
		 //}
	  //}
	 
	  
	  
	  
	 
	  //context.write(new Text(business_id), new IntWritable(1));
	
	  
	 // }

	  
   //Emitting each word as a key and one as its value
	 // while (record.hasMoreTokens())
	  //{
		 //String word= record.nextToken();
		 //System.out.println("key: " + word);
         //context.write(new Text(busID + "," + stars), new IntWritable(1));
  
	  //}

	  //context.write(new Text(busID + "," + stars), new IntWritable(1));


