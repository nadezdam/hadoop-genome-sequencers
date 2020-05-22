package sequencer.algorithm.local_alignment;

import java.io.IOException;
import java.util.Vector;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LocalAlignerReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {

        Vector<String> offsets = new Vector<>();
        for (Text val : values) {
            offsets.add(val.toString());
        }
        context.write(key, new Text(StringUtils.join(offsets, ", ")));
    }
}
