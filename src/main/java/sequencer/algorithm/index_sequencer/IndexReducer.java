package sequencer.algorithm.index_sequencer;

import java.io.IOException;
import java.util.Vector;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexReducer extends Reducer<Text, Text, Text, Text> {

    private String pattern;

    public void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        this.pattern = conf.get("patterns").split(Pattern.quote("|"))[0];

    }

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String subsequence = key.toString();
        Vector<String> offsets = new Vector<>();

        if (pattern.equalsIgnoreCase(subsequence)) {
            for (Text val : values)
                offsets.add(val.toString());

            context.write(new Text(subsequence), new Text(StringUtils.join(offsets, ", ")));
        }
    }
}
