package pkg;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

/**
 * Project: Cosine
 * Class: NewFileTFIDFMapper
 * @author nimrata
 * @date Mar 23, 2017
 **/
public class NewFileTFIDFMapper extends Mapper<LongWritable, Text, Text, Text>{
	//calculated TF and need idf from the previous output
	// for which I will use distributed cache

	public static Map<String,String> newAuthors = new HashMap<String,String>();
	float newtfidf = 0;
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String text = null;
		if (newAuthors.isEmpty()){
			Path path = new Path(context.getConfiguration().get("new_file"));
			FileSystem file = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(file.open(path)));
			while ((text = br.readLine()) != null){
				// getting data from filesystem directly from hdfs by filesystem
				String Doc [] = text.toString().split("\t");
				String word = Doc[1];
				// the 2nd element is WORD
				String TermFreq = Doc[2];
				//the 3rd term is term frequency 
				//save the Word and Term Frequency
				newAuthors.put(word, TermFreq);	

			}

		}
		
		String sentence = value.toString();	
		String[] starray = sentence.split("\t");
		if(starray.length > 1){
			String Idf = starray[1];
			String unigram = starray[0];
			// now get the corresponding term frequency from the map
			if(newAuthors.containsKey(unigram)){
				float NewTf  = Float.valueOf(newAuthors.get(unigram));
				// new term frequency is the the value of unigrams stored in newAuthors
				newtfidf = (float)NewTf * Float.valueOf(Idf);
			}
			else // if the word is not used by that author still there should a 0.5 term freq attached
			{
				newtfidf = (float)(0.5 * Float.valueOf(Idf));
			}
			// output will be word ===> Idf ===> newAAV()
			context.write(new Text (unigram), new Text (Idf + "\t"+ newtfidf));
		}
	}
}

