package pkg;
/**
 * Project: Cosine
 * Class: MainClass
 * @author nimrata
 * @date Mar 23, 2017
 **/
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MainClass {

	private static final String OUT_PATH1="OUTPUTJOB1";
	private static final String OUT_PATH2="OUTPUTJOB2";
	private static final String OUT_PATH3="OUTPUTJOB3";
	private static final String OUTPUT_IDF="OUTPUTJOB4";
	private static final String OUT_PATH4="OUTPUTJOB5";

	public static void main(String[] args) throws IOException, ClassNotFoundException,InterruptedException {
		if (args.length != 3) {
			System.out.printf("Usage: <jar file> <input dir> <WordIDf dir> <output dir>\n");
			System.exit(-1);
		}
		// Job 1 == wordcount

		Configuration conf =new Configuration();
		conf.set("new_file", "hdfs://lincoln:30381" + "/user/nimrata/OUTPUTJOB2/part-r-00000" ); // unknown TF Output>>> Being used by TFIDFMapper
		conf.set("new_TFIDF", "hdfs://lincoln:30381" + "/user/nimrata/OUTPUTJOB3/part-r-00000" ); //unknown TFIDF>>> Being used by AAVRed to calculate Cosine Similarity
		Job wordcount = Job.getInstance(conf);
		wordcount.setJarByClass(MainClass.class);
		wordcount.setMapperClass(UnigramAuthorMapper.class);
		wordcount.setReducerClass(UnigramAuthorReducer.class);
		wordcount.setOutputKeyClass(Text.class);
		wordcount.setOutputValueClass(Text.class);
		wordcount.setInputFormatClass(TextInputFormat.class);
		wordcount.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(wordcount, new Path(args[0])); // path[0] is the unknown file
		FileOutputFormat.setOutputPath(wordcount, new Path(OUT_PATH1));
		if (wordcount.waitForCompletion(true)) {
			System.out.println("JOB ONE(WORDCOUNT) COMPLETION!!");
		}

		//Tf Calculation job Will Start Here
		Job tfcalc=Job.getInstance(conf);
		tfcalc.setJarByClass(MainClass.class);
		tfcalc.setMapperClass(UnigramTFMapper.class);
		tfcalc.setReducerClass(UnigramTFReducer.class);
		tfcalc.setOutputKeyClass(Text.class);
		tfcalc.setOutputValueClass(Text.class);
		tfcalc.setInputFormatClass(TextInputFormat.class);
		tfcalc.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(tfcalc, new Path(OUT_PATH1));
		FileOutputFormat.setOutputPath(tfcalc, new Path(OUT_PATH2));
		if (tfcalc.waitForCompletion(true)){
			System.out.println("JOB(TF CALCULATION) TWO COMPLETION!! ");
		}
		//Tf Calculation job Will Start Here
		Job IDfcalc=Job.getInstance(conf);
		IDfcalc.setJarByClass(MainClass.class);
		IDfcalc.setMapperClass(KnownIdfGenMapper.class);
		IDfcalc.setReducerClass(KnownIdfGenReducer.class);
		IDfcalc.setOutputKeyClass(Text.class);
		IDfcalc.setOutputValueClass(Text.class);
		IDfcalc.setInputFormatClass(TextInputFormat.class);
		IDfcalc.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(IDfcalc, new Path(args[1])); // path[1] is the TFIDF of the gutenberg text  
		FileOutputFormat.setOutputPath(IDfcalc, new Path(OUTPUT_IDF));
		if (IDfcalc.waitForCompletion(true)){
			System.out.println("JOB(IDF CALCULATION) TWO COMPLETION!! ");
		}

		//Tf.Idf Calculation job Will Start Here
		Job tfidfcalc=Job.getInstance(conf);
		tfidfcalc.setJarByClass(MainClass.class);
		tfidfcalc.setMapperClass(NewFileTFIDFMapper.class);
		tfidfcalc.setReducerClass(NewFileTFIDFReducer.class);
		tfidfcalc.setOutputKeyClass(Text.class);
		tfidfcalc.setOutputValueClass(Text.class);
		tfidfcalc.setInputFormatClass(TextInputFormat.class);
		tfidfcalc.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(tfidfcalc, new Path(OUTPUT_IDF)); // takes the IDF of each word and calculates the TFIDF of uknown
		FileOutputFormat.setOutputPath(tfidfcalc, new Path(OUT_PATH3));
		if (tfidfcalc.waitForCompletion(true)) System.out.println("JOB(UNKNOWN TFIDF) THREE COMPLETION!!");

//      job will give tfidf
		Job aavCalc=Job.getInstance(conf);
		aavCalc.setJarByClass(MainClass.class);
		aavCalc.setMapperClass(NewFileAAVMapper.class);
		aavCalc.setReducerClass(NewFileAAVReducer.class);
		aavCalc.setOutputKeyClass(Text.class);
		aavCalc.setOutputValueClass(Text.class);
		aavCalc.setInputFormatClass(TextInputFormat.class);
		aavCalc.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(aavCalc, new Path(args[1]));
		FileOutputFormat.setOutputPath(aavCalc, new Path(OUT_PATH4));
		if (aavCalc.waitForCompletion(true)) System.out.println("JOB FOUR COMPLETION!!");

		// Job will give top 10 authors
		Job top10Authors=Job.getInstance(conf);
		top10Authors.setJarByClass(MainClass.class);
		top10Authors.setMapperClass(Top10AuthorsMapper.class);
		top10Authors.setReducerClass(Top10AuthorsReducer.class);
//		top10Authors.setCombinerClass(Top10AuthorsCombiner.class);
		top10Authors.setOutputKeyClass(Text.class);
		top10Authors.setOutputValueClass(Text.class);
		top10Authors.setInputFormatClass(TextInputFormat.class);
		top10Authors.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(top10Authors, new Path(OUT_PATH4));
		FileOutputFormat.setOutputPath(top10Authors, new Path(args[2]));
		if (top10Authors.waitForCompletion(true)) System.out.println("JOB FIVE COMPLETION!!");
	}
}




