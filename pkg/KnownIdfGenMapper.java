package pkg;
/**
* Project: Cosine
* Class: KnownIdfGenMapper
* @author nimrata
* @date Mar 24, 2017
**/
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class KnownIdfGenMapper extends Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value, Context context) throws
	IOException, InterruptedException{
		// read the output from  profile1b. 1st coulumn is word, 2nd column is author
		// lets just split and send both as key
		
		String text = value.toString().toLowerCase();
		String[] doc = text.split("\t");
		String author = doc[0];
		String word = doc[1];	
		String IDF = doc[3];
	
		
		context.write(new Text(word), new Text(IDF));		
		
		
	}
}
