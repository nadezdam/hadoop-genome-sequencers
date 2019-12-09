package sequencer.algorithm.local_alignment;

import java.io.IOException;
import java.util.Vector;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LocalAlignerReducer extends Reducer<IntWritable, LongWritable, Text, Text> {

    public void reduce(IntWritable key, Iterable<LongWritable> values,
                       Context context) throws IOException, InterruptedException {

        Vector<String> offsets = new Vector<>();
        for (LongWritable val : values) {
            offsets.add(String.valueOf(val.get()));
        }

        context.write(new Text("Score " + key.get() + ": "),
                new Text("Offsets: " + StringUtils.join(offsets, ", ")));
    }
}
