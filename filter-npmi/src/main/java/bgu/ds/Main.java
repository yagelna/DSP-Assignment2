package bgu.ds;

import org.apache.hadoop.fs.Path;

public class Main {
    public static void main(String[] args) {
        FilterNpmi wordCount = new FilterNpmi();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        boolean useCombiner = Boolean.parseBoolean(args[2]);
        long maxSplitSize = Long.parseLong(args[3]);
        double minNpmi = Double.parseDouble(args[4]);
        double relativeMinNpmi = Double.parseDouble(args[5]);
        double thresholdNpmi = Double.parseDouble(args[6]);
        try {
            wordCount.start(input, output, useCombiner, maxSplitSize, minNpmi, relativeMinNpmi, thresholdNpmi);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
