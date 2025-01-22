package bgu.ds;

import org.apache.hadoop.fs.Path;

public class Main {
    public static void main(String[] args) {
        TopN wordCount = new TopN();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        int number = Integer.parseInt(args[2]);
        try {
            wordCount.start(input, output, number);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
