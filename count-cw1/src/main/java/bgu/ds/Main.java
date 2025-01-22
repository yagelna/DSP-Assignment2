package bgu.ds;

import org.apache.hadoop.fs.Path;

public class Main {
    public static void main(String[] args) {
        CountCw1 wordCount = new CountCw1();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        boolean useCombiner = Boolean.parseBoolean(args[2]);
        long maxSplitSize = Long.parseLong(args[3]);
        try {
            wordCount.start(input, output, useCombiner, maxSplitSize);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
