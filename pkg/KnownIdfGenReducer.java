package pkg;
/**
* Project: Cosine
* Class: KnownIdfGenReducer
* @author nimrata
* @date Mar 24, 2017
**/
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KnownIdfGenReducer extends Reducer<Text,Text,Text,Text>{

	

	public void reduce(Text key, Iterable<Text> values, Context context) throws
	IOException, InterruptedException {
		// I want to get idf corresponding to each word without any duplicates.
		String line=null;


		for(Text val: values) {
			line = val.toString();
			
		}
		context.write(new Text (key), new Text(line));
	
	}
}
