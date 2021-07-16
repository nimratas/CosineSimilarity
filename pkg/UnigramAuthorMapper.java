package pkg;
/**
* Project: Cosine
* Class: UnigramAuthorMapper
* @author nimrata
* @date Mar 23, 2017
**/
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class UnigramAuthorMapper extends Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value, Context context) throws
			IOException, InterruptedException {
		String text = "";
		text = value.toString().toLowerCase();
		//extract author's last name, year and sentences by splitting them with <===>

		String author = text.toString().split("<===>")[0];
		String[] fullName = author.split(" ");
		String lastname = fullName[fullName.length - 1].toString(); // to extract author's last name which comes before the first <===>
		String sentence = text.toString().split("<===>")[2];
		String date = text.toString().split("<===>")[1];    // to extract date which comes after <===>
		String[] day = date.split(" ");
		String year = day[day.length - 1].toString(); // year extracted
		//get words from sentence part of the document.

		String noSpecialChars = sentence.replaceAll("[^A-Za-z0-9 ]", "").trim();
		String noSpaces = noSpecialChars.replaceAll("\\s{2,}", " ").trim();
		noSpaces = noSpaces.replaceAll("//s", "");

		String[] words = noSpaces.split(" ");
		for (String word : words) {
			word = word.trim();
			if (word.length() >= 1) {
				context.write(new Text(lastname + "	" + word), new Text("one"));
			}
		}
	}
}