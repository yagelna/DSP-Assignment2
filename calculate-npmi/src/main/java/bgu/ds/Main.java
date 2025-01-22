package bgu.ds;

import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        CalculateNpmi wordCount = new CalculateNpmi();
        List<Path> input = new ArrayList<>();
        input.add(new Path(args[0]));
        input.add(new Path(args[1]));
        input.add(new Path(args[2]));
        Path output = new Path(args[3]);
        try {
            wordCount.start(input, output);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
