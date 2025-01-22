package bgu.ds;

import org.apache.hadoop.fs.Path;

public class Main {
    public static void main(String[] args) {
        CountCw2 wordCount = new CountCw2();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        try {
            wordCount.start(input, output);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
