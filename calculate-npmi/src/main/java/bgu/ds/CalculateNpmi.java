package bgu.ds;

import bgu.ds.common.mapreduce.BigramKeyWritableComparable;
import bgu.ds.common.mapreduce.TaggedValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.List;

public class CalculateNpmi {

    public static class ReducerClass extends Reducer<BigramKeyWritableComparable, TaggedValue, BigramKeyWritableComparable, DoubleWritable> {
        private long decadeCount = 1;
        @Override
        public void reduce(BigramKeyWritableComparable key, Iterable<TaggedValue> values, Context context) throws IOException,  InterruptedException {
            if (key.isDecadeCount()) {
                decadeCount = values.iterator().next().getValue();
                return;
            }

            long cw1w2 = 1, cw1 =1, cw2 = 1;
            for (TaggedValue value : values) {
                switch (value.getTag()) {
                    case BIGRAM_COUNT:
                        cw1w2 = value.getValue();
                        break;
                    case W1_COUNT:
                        cw1 = value.getValue();
                        break;
                    case W2_COUNT:
                        cw2 = value.getValue();
                        break;
                }
            }

            double pw1w2 = (double) cw1w2 / decadeCount;
            double pmi = Math.log(cw1w2) + Math.log(decadeCount) - Math.log(cw1) - Math.log(cw2);
            double npmi = -pmi / Math.log(pw1w2);

            context.write(key, new DoubleWritable(npmi));
        }
    }

    public static class PartitionerClass extends Partitioner<BigramKeyWritableComparable, LongWritable> {
        @Override
        public int getPartition(BigramKeyWritableComparable key, LongWritable value, int numPartitions) {
            return key.getDecade() % numPartitions;
        }
    }

    public void start(List<Path> input, Path output) throws Exception{
        System.out.println("[DEBUG] STEP 4 started!");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Calculate NPMI");
        job.setJarByClass(CalculateNpmi.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(BigramKeyWritableComparable.class);
        job.setMapOutputValueClass(TaggedValue.class);
        job.setOutputKeyClass(BigramKeyWritableComparable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        for (Path path : input) {
            FileInputFormat.addInputPath(job, path);
        }
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
