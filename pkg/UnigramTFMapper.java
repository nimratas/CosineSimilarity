package pkg;
/**
* Project: Cosine
* Class: UnigramTFMapper
* @author nimrata
* @date Mar 23, 2017
**/
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class UnigramTFMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws
			IOException, InterruptedException {
		// read the output from  profile1b. 1st coulumn is word, 2nd column is author
		// lets just split and send both as key
		String text = value.toString().toLowerCase();
		String[] doc = text.split("\t");
		String author = doc[0];
		String word = doc[1];
		String count = doc[2];

		context.write(new Text(author), new Text(word + " " + count));
	}
}