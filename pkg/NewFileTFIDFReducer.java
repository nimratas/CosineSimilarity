package pkg;
/**
* Project: Cosine
* Class: NewFileTFIDFReducer
* @author nimrata
* @date Mar 23, 2017
**/
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NewFileTFIDFReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String tfidf = "";
		String idf = "";
		for (Text val : values) {
			String record = val.toString();
			String[] tfIdfVal = record.split("\t");
			idf = tfIdfVal[0];
			tfidf = tfIdfVal[1];
		}
		context.write(new Text(key.toString()), new Text(idf + "\t" + tfidf));
	}
}
