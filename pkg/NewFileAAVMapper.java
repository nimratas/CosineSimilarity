package pkg;
/**
* Project: Cosine
* Class: NewFileAAVMapper
* @author nimrata
* @date Mar 23, 2017
**/

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

	public class NewFileAAVMapper extends Mapper<LongWritable, Text, Text, Text>{
			
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String sentence = value.toString();
			String[] output = sentence.split("\t");
			if(output.length > 2){
				//The output of mapper will be == Author ==> word==>TF ==> Idf ==> TFIDF
				context.write(new Text(output[0]), new Text (output[1] +"\t"+ output[2] +"\t"+ output[3] +"\t"+ output[4]));
			}
		}
	}