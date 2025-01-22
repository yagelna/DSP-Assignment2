package bgu.ds;

import org.apache.hadoop.fs.Path;

public class Main {
    public static void main(String[] args) {
        FilterNpmi wordCount = new FilterNpmi();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        double minNpmi = Double.parseDouble(args[2]);
        double relativeMinNpmi = Double.parseDouble(args[3]);
        double thresholdNpmi = Double.parseDouble(args[4]);
        try {
            wordCount.start(input, output, minNpmi, relativeMinNpmi, thresholdNpmi);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
