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

    public static class ReducerClass extends Reducer<BigramKeyWritableComparable, DoubleWritable, BigramKeyWritableComparable, DoubleWritable> {
        private Map<Integer, Integer> decadesMap = new HashMap<>();

        @Override
        public void reduce(BigramKeyWritableComparable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            decadesMap.putIfAbsent(key.getDecade(), 0);
            int count = decadesMap.get(key.getDecade());
            if (count < context.getConfiguration().getInt("top.number", 10)) {
                context.write(key, values.iterator().next());
                decadesMap.put(key.getDecade(), count + 1);
            }
        }
    }

    public static class PartitionerClass extends Partitioner<BigramKeyWritableComparable, DoubleWritable> {
        @Override
        public int getPartition(BigramKeyWritableComparable key, DoubleWritable value, int numPartitions) {
            return key.getDecade() % numPartitions;
        }
    }

    public void start(Path input, Path output, int number) throws Exception{
        System.out.println("[DEBUG] STEP 7 started!");
        Configuration conf = new Configuration();
        conf.setInt("top.number", number);

        Job job = Job.getInstance(conf, "Top N NPMI");
        job.setJarByClass(TopN.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(BigramKeyWritableComparable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(BigramKeyWritableComparable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
