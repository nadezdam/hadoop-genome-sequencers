package sequencer.algorithm.index_sequencer;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    private int patternLength;

    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        String pattern = conf.get("patterns").split(Pattern.quote("|"))[0];
        System.out.println(pattern);
        this.patternLength = pattern.length();
    }

    public void map(LongWritable key, Text value, Context context) {
        try {

            String sequence = value.toString();
            for (int i = 0; i < sequence.length() - patternLength + 1; i++) {
                String subsequence = sequence.substring(i, i + patternLength);
                long offset = key.get() + i;
                context.write(new Text(subsequence), new Text(String.valueOf(offset)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
