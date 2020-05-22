import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.jute.Index;
import sequencer.algorithm.approximate_match_sequencer.ApproximateMatchMapper;
import sequencer.algorithm.approximate_match_sequencer.ApproximateMatchReducer;
import sequencer.algorithm.boyer_moore_sequencer.BoyerMooreMapper;
import sequencer.algorithm.boyer_moore_sequencer.BoyerMooreReducer;
import sequencer.algorithm.index_sequencer.*;
import sequencer.algorithm.local_alignment.LocalAlignerMapper;
import sequencer.algorithm.local_alignment.LocalAlignerReducer;

class SequencerAlgorithm {
    private Class mapperClass;
    private Class reducerClass;
    private Class outputKeyClass;
    private Class outputValueClass;

    SequencerAlgorithm(String algorithm) {
        switch (algorithm) {
            case "index-sequencer":
                this.mapperClass = IndexMapper.class;
                this.reducerClass = IndexReducer.class;
                this.outputKeyClass = Text.class;
                this.outputValueClass = Text.class;
                break;
            case "boyer-moore-sequencer":
                this.mapperClass = BoyerMooreMapper.class;
                this.reducerClass = BoyerMooreReducer.class;
                this.outputKeyClass = Text.class;
                this.outputValueClass = Text.class;
                break;
            case "approximate-match-sequencer":
                this.mapperClass = ApproximateMatchMapper.class;
                this.reducerClass = ApproximateMatchReducer.class;
                this.outputKeyClass = Text.class;
                this.outputValueClass = Text.class;
                break;
            case "local-alignment":
                this.mapperClass = LocalAlignerMapper.class;
                this.reducerClass = LocalAlignerReducer.class;
                this.outputKeyClass = Text.class;
                this.outputValueClass = Text.class;
                break;
        }
    }

    Class getMapperClass() {
        return mapperClass;
    }

    Class getReducerClass() {
        return reducerClass;
    }

    Class getOutputKeyClass() {
        return outputKeyClass;
    }

    Class getOutputValueClass() {
        return outputValueClass;
    }
}
