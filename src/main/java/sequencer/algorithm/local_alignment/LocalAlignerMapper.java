package sequencer.algorithm.local_alignment;

import java.io.IOException;
import java.util.regex.Pattern;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LocalAlignerMapper extends
        Mapper<LongWritable, Text, IntWritable, LongWritable> {

    private String pattern;
    private int score_limit;

    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        this.pattern = conf.get("pattern");
//                conf.get("patterns").split(Pattern.quote("|"))[0];
        this.score_limit = conf.getInt("score-limit", 0);
    }

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        long base_offset = key.get();
        String sequence = value.toString();
        int offset;
        int score;
        SmithWaterman aligner = new SmithWaterman();
        aligner.SetSequences(sequence, pattern);
        aligner.SmithWatermanAlgorithm();

        offset = aligner.offset;
        score = aligner.score;

        if (score >= score_limit) {
            context.write(new IntWritable(score), new LongWritable(base_offset + offset));
        }

    }
}
