package sequencer.algorithm.boyer_moore_sequencer;

import java.io.IOException;
import java.util.Vector;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BoyerMooreReducer extends Reducer<Text, Text, Text, Text> {


	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String pattern = key.toString();
		Vector<String> offsets = new Vector<>();
		
		for (Text val : values)
			offsets.add(val.toString());

		context.write(new Text("Pattern: " + pattern), new Text("\nOffsets: \n"
				+ StringUtils.join(offsets, ", ")));
	}
}
