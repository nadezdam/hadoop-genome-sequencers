package sequencer.algorithm.approximate_match_sequencer;

import java.io.IOException;
import java.util.Vector;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ApproximateMatchReducer extends
        Reducer<IntWritable, LongWritable, Text, Text> {

    public void reduce(IntWritable key, Iterable<LongWritable> values,
                       Context context) throws IOException, InterruptedException {

        Vector<String> offsets = new Vector<>();
        for (LongWritable val : values) {
            offsets.add(String.valueOf(val.get()));
        }

        context.write(new Text("Number of edits:" + key.get()), new Text(
                StringUtils.join(offsets, ", ")));

    }
}
