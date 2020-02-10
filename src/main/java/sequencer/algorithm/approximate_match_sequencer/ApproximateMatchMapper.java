package sequencer.algorithm.approximate_match_sequencer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ApproximateMatchMapper extends
        Mapper<LongWritable, Text, IntWritable, LongWritable> {

    private String pattern;
    private int edit_limit;

    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        this.pattern = conf.get("pattern");
//                conf.get("patterns").split(Pattern.quote("|"))[0];
        this.edit_limit = conf.getInt("edit-limit", 0);
    }

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String sequence = value.toString();
        int min_edits;
        long base_offset = key.get();
        ArrayList<Integer> offsets;

        ApproximateMatcher am = new ApproximateMatcher(pattern, sequence);
        offsets = am.offsets;
        min_edits = am.min_edit_el;

        if (min_edits <= edit_limit) {
            for (int offset : offsets) {
                context.write(new IntWritable(min_edits), new LongWritable(
                        base_offset + (long) offset));
            }
        }


    }
}
