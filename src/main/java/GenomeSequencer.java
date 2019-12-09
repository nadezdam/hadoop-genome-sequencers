import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import sequence.input.format.*;

public class GenomeSequencer extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new GenomeSequencer(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
//        if (args.length != 1) {
//            System.out.println("Usage: <sequencer-algorithm>");
//            return 1;
//        }
        Configuration conf = new Configuration();
        InputStream propertiesInputFile = getClass().getClassLoader().getResourceAsStream("config.properties");
        Properties properties = new Properties();
        properties.load(propertiesInputFile);

        String referenceGenomePath = "";
        try {
            referenceGenomePath = properties.getProperty("input");
        } catch (Exception exception) {
            System.out.println(exception.getMessage());
            System.out.println("Path to reference genome must be set!");
        }

        Path resultsFolderPath = new Path(properties.getProperty("output", "./results"));
        String patterns = properties.getProperty("patterns");
        String sequencerAlgorithm=properties.getProperty("sequencer-algorithm");
        int patternLength = Integer.parseInt(properties.getProperty("pattern-length"));
        int numReduceTasks = Integer.parseInt(properties.getProperty("num-reduce-tasks"));
        int editLimit = Integer.parseInt(properties.getProperty("edit-limit"));
        int scoreLimit = Integer.parseInt(properties.getProperty("score-limit"));

        conf.set("patterns", patterns);
        conf.setInt("patternLength", patternLength);
        conf.setInt("edit-limit", editLimit);
        conf.setInt("score-limit", scoreLimit);
        FileSystem hdfs = FileSystem.get(conf);

        if (hdfs.exists(resultsFolderPath)) {
            hdfs.delete(resultsFolderPath, true);
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(GenomeSequencer.class);
        SequencerAlgorithm sequencerAlgorithmInstance = new SequencerAlgorithm(sequencerAlgorithm);

        job.setOutputKeyClass(sequencerAlgorithmInstance.getOutputKeyClass());
        job.setOutputValueClass(sequencerAlgorithmInstance.getOutputValueClass());

        job.setMapperClass(sequencerAlgorithmInstance.getMapperClass());
        job.setReducerClass(sequencerAlgorithmInstance.getReducerClass());
        job.setNumReduceTasks(numReduceTasks);

        FileInputFormat.setInputPaths(job, new Path(referenceGenomePath));
        FileOutputFormat.setOutputPath(job, resultsFolderPath);

        job.setInputFormatClass(SequenceInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }


}
