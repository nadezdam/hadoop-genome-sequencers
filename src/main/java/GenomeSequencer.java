import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
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

        if (args.length != 7) {
            System.out.println("usage: -input -output -algorithm -pattern -numReduceTasks -numMapTasks (if 0, then use default) -useCombiner (boolean)");
            return 0;
        }

        Path referenceGenomePath = new Path(args[0]);

        String sequencerAlgorithm = args[2];
        Path resultsFolderPath = new Path(args[1] + "-" + sequencerAlgorithm);
        String pattern = args[3];
        long patternLen = pattern.length();
        int numReduceTasks = Integer.parseInt(args[4]);
        int DESIRED_NUM_OF_SPLITS = Integer.parseInt(args[5]);
        boolean useCombiner = Boolean.parseBoolean(args[6]);
        final long DEFAULT_SPLIT_SIZE = 128 * 1024 * 1024;
//        int editLimit = Integer.parseInt(args[5]);
//        int scoreLimit = Integer.parseInt(args[6]);

        int editLimit = (int) Math.ceil(patternLen * 0.2);
        int scoreLimit = (int) Math.ceil(patternLen);

        Configuration conf = new Configuration();
        conf.set("pattern", pattern);
        conf.setInt("patternLength", pattern.length());
        conf.setInt("edit-limit", editLimit);
        conf.setInt("score-limit", scoreLimit);

        FileSystem hdfs = FileSystem.get(conf);

        if (DESIRED_NUM_OF_SPLITS != 0) {
            ContentSummary cSummary = hdfs.getContentSummary(referenceGenomePath);
            long FILE_SIZE = cSummary.getLength();
            long SPLIT_SIZE = FILE_SIZE / DESIRED_NUM_OF_SPLITS;
            conf.setLong(SequenceInputFormat.SPLIT_MAXSIZE, SPLIT_SIZE);
        }

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

        if (useCombiner) {
            job.setJobName(sequencerAlgorithm + " w/ combiner");
            job.setCombinerClass(sequencerAlgorithmInstance.getReducerClass());
        } else {
            job.setJobName(sequencerAlgorithm + " w/o combiner");
        }

        job.setNumReduceTasks(numReduceTasks);
        FileInputFormat.setInputPaths(job, referenceGenomePath);
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
