package bgu.ds;

import bgu.ds.common.mapreduce.BigramKeyWritableComparable;
import bgu.ds.common.mapreduce.TagEnum;
import bgu.ds.common.mapreduce.TaggedValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CountCw2 {

    public static class MapperClass extends Mapper<BigramKeyWritableComparable, TaggedValue, BigramKeyWritableComparable, LongWritable> {
        private LongWritable zero = new LongWritable(0);

        @Override
        public void map(BigramKeyWritableComparable key, TaggedValue value, Context context) throws IOException,  InterruptedException {
            if (key.isBigramCount()) {
                context.write(new BigramKeyWritableComparable(key.getDecade(), key.getW2(), key.getW1()), zero);
                context.write(new BigramKeyWritableComparable(key.getDecade(), key.getW2()), new LongWritable(value.getValue()));
            }
        }
    }

    public static class CombinerClass extends Reducer<BigramKeyWritableComparable, LongWritable, BigramKeyWritableComparable, LongWritable> {
        @Override
        public void reduce(BigramKeyWritableComparable key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static class ReducerClass extends Reducer<BigramKeyWritableComparable, LongWritable, BigramKeyWritableComparable, TaggedValue> {
        private long sumCw2 = 0;

        @Override
        public void reduce(BigramKeyWritableComparable key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            if (key.isUnigramCount()) {
                sumCw2 = 0;
                for (LongWritable value : values) {
                    sumCw2 += value.get();
                }
            } else if (key.isBigramCount()){
                context.write(new BigramKeyWritableComparable(key.getDecade(), key.getW2(), key.getW1()), new TaggedValue(TagEnum.W2_COUNT, sumCw2));
            }
        }
    }

    public static class PartitionerClass extends Partitioner<BigramKeyWritableComparable, LongWritable> {
        @Override
        public int getPartition(BigramKeyWritableComparable key, LongWritable value, int numPartitions) {
            return ((key.getDecade() + key.getW1().hashCode()) & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public void start(Path input, Path output, boolean useCombiner, long maxSplitSize) throws Exception{
        System.out.println("[DEBUG] STEP 3 started!");
        Configuration conf = new Configuration();
        if (maxSplitSize > 0)
            conf.setLong("mapred.max.split.size", maxSplitSize);

        Job job = Job.getInstance(conf, "Count C(w2)");
        job.setJarByClass(CountCw2.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        if (useCombiner)
            job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(BigramKeyWritableComparable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(BigramKeyWritableComparable.class);
        job.setOutputValueClass(TaggedValue.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
