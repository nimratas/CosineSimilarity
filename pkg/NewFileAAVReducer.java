package pkg;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class NewFileAAVReducer extends Reducer<Text,Text,Text,Double>{

	public static Map<String, String> uAuthors = new HashMap<String, String>();  //save words and their frequencies
	public static Map<String, String> kAuthors =new HashMap<String, String>();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String line = null;
		if (uAuthors.isEmpty()){
			// I want to read the TFIDF of known authors
			Path path = new Path(context.getConfiguration().get("new_TFIDF"));
			FileSystem file = FileSystem.get(new Configuration());
			BufferedReader buffereader = new BufferedReader(new InputStreamReader(file.open(path)));
		
			while ((line = buffereader.readLine()) != null){
				String array[] =  line.toString().split("\t");
				uAuthors.put(array[0],(array[1]+"\t"+array[2]));
			}
			buffereader.close();
		} 

		for(Text val: values) {
			String parsedString = val.toString();
			String[] ValArray = parsedString.split("\t");
			kAuthors.put(ValArray[0],ValArray[3]);
		} 
		float sum = 0, A = 0, B=0;
		double cosine=0;
		for(String Uword:uAuthors.keySet()){
			if(kAuthors.containsKey(Uword)){
				float tempA = Float.valueOf(kAuthors.get(Uword));
				String str = uAuthors.get(Uword);
				String Bvalue[] = str.split("\t");
				float tempB = Float.valueOf(Bvalue[1]);
				sum = sum + (tempA*tempB);
				A = (float) (A + Math.pow(tempA, 2));
				B = (float) (B + Math.pow(tempB, 2));
			}
			else
			{
				String BS = uAuthors.get(Uword);
				String Bvalue[] = BS.split("\t");
				float tempB = Float.valueOf(Bvalue[1]);
				float tempA = (float) (0.5*Float.valueOf(Bvalue[0]));
				sum = sum + (tempA*tempB);
				A = (float) (A + Math.pow(tempA, 2));
				B = (float) (B + Math.pow(tempB, 2));	
			}
		}
		cosine =(double)(sum/(float)(Math.sqrt(A)*Math.sqrt(B)));
		context.write(new Text(key.toString()), (cosine));
	}
}
