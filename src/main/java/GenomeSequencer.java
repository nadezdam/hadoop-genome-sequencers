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

import javax.sound.midi.Sequence;

public class GenomeSequencer extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new GenomeSequencer(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
//        InputStream propertiesInputFile = getClass().getClassLoader().getResourceAsStream("config.properties");
//        Properties properties = new Properties();
//        properties.load(propertiesInputFile);

//        String referenceGenomePath = "";
//        try {
//            referenceGenomePath = properties.getProperty("input");
//        } catch (Exception exception) {
//            System.out.println(exception.getMessage());
//            System.out.println("Path to reference genome must be set!");
//        }

//        Path resultsFolderPath = new Path(properties.getProperty("output", "./results"));
//        String pattern = properties.getProperty("pattern");
//        String sequencerAlgorithm = properties.getProperty("sequencer-algorithm");
//        int numReduceTasks = Integer.parseInt(properties.getProperty("num-reduce-tasks"));
//        int editLimit = Integer.parseInt(properties.getProperty("edit-limit"));
//        int scoreLimit = Integer.parseInt(properties.getProperty("score-limit"));

        String referenceGenomePath = args[0];

        Path resultsFolderPath = new Path(args[1]);
        String pattern = args[2];
        long patternLen = pattern.length();
        String sequencerAlgorithm = args[3];
        int numReduceTasks = Integer.parseInt(args[4]);
//        int editLimit = Integer.parseInt(args[5]);
//        int scoreLimit = Integer.parseInt(args[6]);

        int editLimit = (int) Math.ceil(patternLen * 0.1);
        int scoreLimit = (int) Math.ceil(patternLen);

        Configuration conf = new Configuration();
        conf.set("pattern", pattern);
        conf.setInt("patternLength", pattern.length());
        conf.setInt("edit-limit", editLimit);
        conf.setInt("score-limit", scoreLimit);
        FileSystem hdfs = FileSystem.get(conf);
        long FILE_SIZE = (long) 9.3 * 1024 * 1024 * 1024;
//        int DESIRED_NUM_OF_SPLITS = 40;
        int DESIRED_NUM_OF_SPLITS = 20;
//        int DESIRED_NUM_OF_SPLITS = 60;
        long SPLIT_SIZE = FILE_SIZE / DESIRED_NUM_OF_SPLITS;

//        final long DEFAULT_SPLIT_SIZE = 128 * 1024 * 1024;
        conf.setLong(SequenceInputFormat.SPLIT_MAXSIZE, SPLIT_SIZE);
        if (hdfs.exists(resultsFolderPath)) {
            hdfs.delete(resultsFolderPath, true);
        }
        Job job = Job.getInstance(conf);
        job.setJarByClass(GenomeSequencer.class);
        SequencerAlgorithm sequencerAlgorithmInstance = new SequencerAlgorithm(sequencerAlgorithm);

        job.setOutputKeyClass(sequencerAlgorithmInstance.getOutputKeyClass());
        job.setOutputValueClass(sequencerAlgorithmInstance.getOutputValueClass());

        job.setMapperClass(sequencerAlgorithmInstance.getMapperClass());
        job.setCombinerClass(sequencerAlgorithmInstance.getReducerClass());
        job.setReducerClass(sequencerAlgorithmInstance.getReducerClass());
        job.setNumReduceTasks(numReduceTasks);

        FileInputFormat.setInputPaths(job, new Path(referenceGenomePath));
        FileOutputFormat.setOutputPath(job, resultsFolderPath);

        job.setInputFormatClass(SequenceInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        long start = System.currentTimeMillis();
        int jobExitResult = job.waitForCompletion(true) ? 0 : 1;
        long end = System.currentTimeMillis();
        long elapsed = (end - start) / 1000;
        System.out.println("Execution time in seconds: " + elapsed);
        return jobExitResult;
    }


}
