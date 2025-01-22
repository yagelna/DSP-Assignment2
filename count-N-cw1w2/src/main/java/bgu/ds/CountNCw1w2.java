package bgu.ds;

import bgu.ds.common.mapreduce.BigramKeyWritableComparable;
import bgu.ds.common.mapreduce.TagEnum;
import bgu.ds.common.mapreduce.TaggedValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class CountNCw1w2 {

    public static class MapperClass extends Mapper<LongWritable, Text, BigramKeyWritableComparable, LongWritable> {
        private Set<String> stopWords = new HashSet<>();

        @Override
        public void setup(Context context) {
            System.out.println("[DEBUG] Mapper setup");
            byte[] stopWords = S3ObjectOperations.getInstance().getObjectAsByteArray(
                    context.getConfiguration().get("stop_words.bucket"),
                    context.getConfiguration().get("stop_words.key"));
            System.out.println("[DEBUG] Stop words: " + new String(stopWords));
            Collections.addAll(this.stopWords, new String(stopWords).split("\n"));
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");

            Text bigrams = new Text(tokens[0]);
            String[] words = bigrams.toString().split("\\s+");

            if (words.length != 2 || stopWords.contains(words[0]) || stopWords.contains(words[1])) {
                return;
            }

            int decade = (Integer.parseInt(tokens[1]) / 10);
            LongWritable count = new LongWritable(Integer.parseInt(tokens[2]));

            context.write(new BigramKeyWritableComparable(decade), count);
            context.write(new BigramKeyWritableComparable(decade, words[0], words[1]), count);
        }
    }

    public static class CombinerClass extends Reducer<BigramKeyWritableComparable, LongWritable, BigramKeyWritableComparable, LongWritable> {
        @Override
        public void reduce(BigramKeyWritableComparable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static class ReducerClass extends Reducer<BigramKeyWritableComparable, LongWritable, BigramKeyWritableComparable, TaggedValue> {
        @Override
        public void reduce(BigramKeyWritableComparable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new TaggedValue(TagEnum.BIGRAM_COUNT, sum));
        }
    }

    public static class PartitionerClass extends Partitioner<BigramKeyWritableComparable, LongWritable> {
        @Override
        public int getPartition(BigramKeyWritableComparable key, LongWritable value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public void start(Path input, Path output, boolean useCombiner, long maxSplitSize, String StopWordsBucket, String StopWordsKey) throws Exception{
        System.out.println("[DEBUG] STEP 1 started!");

        Configuration conf = new Configuration();
        if (maxSplitSize > 0)
            conf.setLong("mapred.max.split.size", maxSplitSize);
        conf.set("stop_words.bucket", StopWordsBucket);
        conf.set("stop_words.key", StopWordsKey);

        Job job = Job.getInstance(conf, "Count N and C(w1,w2)");
        job.setJarByClass(CountNCw1w2.class);
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
