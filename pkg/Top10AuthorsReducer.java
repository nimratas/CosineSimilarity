package pkg;
/**
 * Project: Cosine
 * Class: Top10AuthorsReducer
 * @author nimrata
 * @date Mar 25, 2017
 **/
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * Project: Cosine
 * Class: Top10AuthorsCombiner
 * @author nimrata
 * @date Mar 25, 2017
 **/
public class Top10AuthorsReducer extends Reducer<Text, Text, Text, Text> {
	TreeMap<Float, String> treeMap = new TreeMap<Float, String>();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			String[] parsed = value.toString().split("\t");
			treeMap.put(Float.parseFloat(parsed[1]), parsed[0].toString());

			if (treeMap.size() > 10) {
				treeMap.remove(treeMap.firstKey());
			}
		}
		for (String t : treeMap.descendingMap().values()) {
			context.write(new Text(t), new Text(""));
		}
	}
}



