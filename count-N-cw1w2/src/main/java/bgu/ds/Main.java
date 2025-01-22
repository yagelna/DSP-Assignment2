package bgu.ds;

import org.apache.hadoop.fs.Path;

public class Main {
    public static void main(String[] args) {
        CountNCw1w2 wordCount = new CountNCw1w2();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        boolean useCombiner = Boolean.parseBoolean(args[2]);
        long maxSplitSize = Long.parseLong(args[3]);
        String stopWordsBucket = args[4];
        String stopWordsKey = args[5];
        double sampleRate = Double.parseDouble(args[6]);
        try {
            wordCount.start(input, output, useCombiner, maxSplitSize, stopWordsBucket, stopWordsKey, sampleRate);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
