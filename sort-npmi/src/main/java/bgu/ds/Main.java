package bgu.ds;

import org.apache.hadoop.fs.Path;

public class Main {
    public static void main(String[] args) {
        SortNpmi wordCount = new SortNpmi();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        try {
            wordCount.start(input, output);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
