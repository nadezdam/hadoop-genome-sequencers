package sequencer.algorithm.boyer_moore_sequencer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class BoyerMooreMapper extends
        Mapper<LongWritable, Text, Text, Text> {

    private String pattern;
    private BoyerMoore bm_object;

    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        this.pattern = conf.get("pattern");
//                conf.get("patterns").split(Pattern.quote("|"))[0];
        this.bm_object = new BoyerMoore(pattern);
    }

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String sequence = value.toString();
        long base_offset = key.get();

        ArrayList<Integer> matches = bm_object.searchPattern(sequence);

        for (Integer match : matches) {
            long offset = base_offset + match;
            context.write(new Text(pattern), new Text(String.valueOf(offset)));
        }
    }

}
