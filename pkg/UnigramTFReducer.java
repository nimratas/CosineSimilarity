package pkg;
/**
* Project: Cosine
* Class: UnigramTFReducer
* @author nimrata
* @date Mar 23, 2017
**/
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class UnigramTFReducer extends Reducer<Text,Text,Text,Text>{

	public void reduce(Text key, Iterable<Text> values, Context context) throws
			IOException, InterruptedException {
		ArrayList<String> wordwithCount = new ArrayList<String>();
		// maxFreq is maximum number of times a word appears for a single author
		int maxfreq = 0;
		for (Text val : values) {
			wordwithCount.add(val.toString());
			String[] wordandCount = val.toString().split(" ");

			int frequency = Integer.parseInt(wordandCount[1]);
			if (frequency > maxfreq) {
				maxfreq = frequency;
			}
		}
		// maintaining authorcount
		// if author does not contain this author add him as key
		for (String entry : wordwithCount) {
			String[] wordandCount = entry.toString().split(" ");
			int frequency = Integer.parseInt(wordandCount[1]);
			float temp = frequency / (float) maxfreq;
			float TermFreq = (float) (0.5 + 0.5 * temp);
			//The output of TF reducer will be authorname --> word--> TF
			context.write(key, new Text(wordandCount[0] + "\t" + TermFreq));
		}
	}
}
