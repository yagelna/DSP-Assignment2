package bgu.ds;

import bgu.ds.common.mapreduce.BigramKeyWritableComparable;
import bgu.ds.common.mapreduce.TagEnum;
import bgu.ds.common.mapreduce.TaggedValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class FilterNpmi {

    public static class MapperClass extends Mapper<BigramKeyWritableComparable, DoubleWritable, BigramKeyWritableComparable, DoubleWritable> {

        @Override
        public void map(BigramKeyWritableComparable key, DoubleWritable value, Context context) throws IOException,  InterruptedException {
            context.write(new BigramKeyWritableComparable(key.getDecade()), new DoubleWritable(value.get()));
            context.write(key, value);
        }
    }

    public static class CombinerClass extends Reducer<BigramKeyWritableComparable, DoubleWritable, BigramKeyWritableComparable, DoubleWritable> {
        @Override
        public void reduce(BigramKeyWritableComparable key, Iterable<DoubleWritable> values, Context context) throws IOException,  InterruptedException {
            double sum = 0;
            for (DoubleWritable value : values) {
                sum += value.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }

    public static class ReducerClass extends Reducer<BigramKeyWritableComparable, DoubleWritable, BigramKeyWritableComparable, DoubleWritable> {
        private double decadeNpmiSum = 0;
        private double minPmi;
        private double relativeMinPmi;
        private double thresholdNpmi;

        @Override
        public void setup(Context context) {
            minPmi = Double.parseDouble(context.getConfiguration().get("minPmi", "1.0"));
            relativeMinPmi = Double.parseDouble(context.getConfiguration().get("relativeMinPmi", "1.0"));
            thresholdNpmi = Double.parseDouble(context.getConfiguration().get("thresholdNpmi", "0.99999"));
        }

        @Override
        public void reduce(BigramKeyWritableComparable key, Iterable<DoubleWritable> values, Context context) throws IOException,  InterruptedException {
            if (key.isDecadeCount()) {
                for (DoubleWritable value : values) {
                    decadeNpmiSum += value.get();
                }
            } else {
                double npmi = values.iterator().next().get();
                if (npmi <  thresholdNpmi && (npmi >= minPmi || npmi >= relativeMinPmi * decadeNpmiSum)) {
                    context.write(key, new DoubleWritable(npmi));
                }
            }
        }
    }

    public static class PartitionerClass extends Partitioner<BigramKeyWritableComparable, DoubleWritable> {
        @Override
        public int getPartition(BigramKeyWritableComparable key, DoubleWritable value, int numPartitions) {
            return key.getDecade() % numPartitions;
        }
    }

    public void start(Path input, Path output, double minPmi, double relativeMinPmi, double thresholdNpmi) throws Exception{
        System.out.println("[DEBUG] STEP 5 started!");
        Configuration conf = new Configuration();
        conf.set("minPmi", Double.toString(minPmi));
        conf.set("relativeMinPmi", Double.toString(relativeMinPmi));
        conf.set("thresholdNpmi", Double.toString(thresholdNpmi));

        Job job = Job.getInstance(conf, "Filter NPMI");
        job.setJarByClass(FilterNpmi.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(CombinerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(BigramKeyWritableComparable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(BigramKeyWritableComparable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
