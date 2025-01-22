package bgu.ds;

import bgu.ds.common.mapreduce.BigramKeyWritableComparable;
import bgu.ds.common.mapreduce.DecadeNpmiWritableComparable;
import bgu.ds.common.mapreduce.TaggedValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class SortNpmi {

    public static class MapperClass extends Mapper<BigramKeyWritableComparable, DoubleWritable, DecadeNpmiWritableComparable, BigramKeyWritableComparable> {

        @Override
        public void map(BigramKeyWritableComparable key, DoubleWritable value, Context context) throws IOException,  InterruptedException {
            context.write(new DecadeNpmiWritableComparable(key.getDecade(), value.get() * -1), key);
        }
    }

    public static class ReducerClass extends Reducer<DecadeNpmiWritableComparable, BigramKeyWritableComparable, BigramKeyWritableComparable, DoubleWritable> {
        @Override
        public void reduce(DecadeNpmiWritableComparable key, Iterable<BigramKeyWritableComparable> values, Context context) throws IOException,  InterruptedException {
            for (BigramKeyWritableComparable value : values) {
                context.write(value, new DoubleWritable(key.getNpmi() * -1));
            }
        }
    }

    public static class PartitionerClass extends Partitioner<DecadeNpmiWritableComparable, BigramKeyWritableComparable> {
        @Override
        public int getPartition(DecadeNpmiWritableComparable key, BigramKeyWritableComparable value, int numPartitions) {
            return key.getDecade() % numPartitions;
        }
    }

    public void start(Path input, Path output) throws Exception{
        System.out.println("[DEBUG] STEP 6 started!");
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Sort NPMI");
        job.setJarByClass(SortNpmi.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(DecadeNpmiWritableComparable.class);
        job.setMapOutputValueClass(BigramKeyWritableComparable.class);
        job.setOutputKeyClass(BigramKeyWritableComparable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
