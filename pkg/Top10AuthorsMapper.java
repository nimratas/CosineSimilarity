package pkg;
/**
 * Project: Cosine
 * Class: Top10AuthorsMapper
 * @author nimrata
 * @date Mar 25, 2017
 **/
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Top10AuthorsMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws
			IOException, InterruptedException {
		context.write(new Text(""), new Text(value));
	}

}


