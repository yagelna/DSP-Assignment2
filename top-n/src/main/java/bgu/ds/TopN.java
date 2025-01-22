package bgu.ds;

import bgu.ds.common.mapreduce.BigramKeyWritableComparable;
import bgu.ds.common.mapreduce.DecadeNpmiWritableComparable;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class TopN {

    public static class MapperClass extends Mapper<BigramKeyWritableComparable, DoubleWritable, DecadeNpmiWritableComparable, BigramKeyWritableComparable> {

        @Override
        public void map(BigramKeyWritableComparable key, DoubleWritable value, Context context) throws IOException,  InterruptedException {
            context.write(new DecadeNpmiWritableComparable(key.getDecade(), value.get() * -1), key);
        }
    }

    public static class ReducerClass extends Reducer<DecadeNpmiWritableComparable, BigramKeyWritableComparable, BigramKeyWritableComparable, DoubleWritable> {
        private Map<Integer, Integer> decadesMap = new HashMap<>();

        @Override
        public void reduce(DecadeNpmiWritableComparable key, Iterable<BigramKeyWritableComparable> values, Context context) throws IOException, InterruptedException {
            decadesMap.putIfAbsent(key.getDecade(), 0);
            int count = decadesMap.get(key.getDecade());
            for (BigramKeyWritableComparable value : values) {
                if (count < context.getConfiguration().getInt("top.number", 10)) {
                    context.write(value, new DoubleWritable(key.getNpmi() * -1));
                    count++;
                }
            }
            decadesMap.put(key.getDecade(), count);
        }
    }

    public static class PartitionerClass extends Partitioner<DecadeNpmiWritableComparable, BigramKeyWritableComparable> {
        @Override
        public int getPartition(DecadeNpmiWritableComparable key, BigramKeyWritableComparable value, int numPartitions) {
            return key.getDecade() % numPartitions;
        }
    }

    public void start(Path input, Path output, long maxSplitSize, int number) throws Exception{
        System.out.println("[DEBUG] STEP 7 started!");
        Configuration conf = new Configuration();
        if (maxSplitSize > 0)
            conf.setLong("mapred.max.split.size", maxSplitSize);
        conf.setInt("top.number", number);

        Job job = Job.getInstance(conf, "Top N NPMI");
        job.setJarByClass(TopN.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(DecadeNpmiWritableComparable.class);
        job.setMapOutputValueClass(BigramKeyWritableComparable.class);
        job.setOutputKeyClass(BigramKeyWritableComparable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
