package bgu.ds;

import org.apache.hadoop.fs.Path;

public class Main {
    public static void main(String[] args) {
        CountNCw1w2 wordCount = new CountNCw1w2();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        String stopWordsBucket = args[2];
        String stopWordsKey = args[3];

        try {
            wordCount.start(input, output, stopWordsBucket, stopWordsKey);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
