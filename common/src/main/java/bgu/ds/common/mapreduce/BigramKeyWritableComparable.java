package bgu.ds.common.mapreduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BigramKeyWritableComparable implements WritableComparable<BigramKeyWritableComparable> {
    private int decade;
    private String w1;
    private String w2;

    public BigramKeyWritableComparable() {
        this.decade = 0;
        this.w1 = "";
        this.w2 = "";
    }

    public BigramKeyWritableComparable(int decade) {
        this.decade = decade;
        this.w1 = "";
        this.w2 = "";
    }

    public BigramKeyWritableComparable(int decade, String w1) {
        this.decade = decade;
        this.w1 = w1;
        this.w2 = "";
    }

    public BigramKeyWritableComparable(int decade, String w1, String w2) {
        this.decade = decade;
        this.w1 = w1;
        this.w2 = w2;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(decade);
        dataOutput.writeUTF(w1);
        dataOutput.writeUTF(w2);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        decade = dataInput.readInt();
        w1 = dataInput.readUTF();
        w2 = dataInput.readUTF();
    }

    @Override
    public int compareTo(BigramKeyWritableComparable o) {
        if (decade != o.decade) {
            return decade - o.decade;
        }

        if (w1.compareTo(o.w1) != 0) {
            return w1.compareTo(o.w1);
        }

        return w2.compareTo(o.w2);
    }

    public int hashCode() {
        return decade + w1.hashCode() + w2.hashCode();
    }

    public int getDecade() {
        return decade;
    }

    public String getW1() {
        return w1;
    }

    public String getW2() {
        return w2;
    }

    public boolean isDecadeCount() {
        return w1.isEmpty();
    }

    public boolean isUnigramCount() {
        return !w1.isEmpty() && w2.isEmpty();
    }

    public boolean isBigramCount() {
        return !w1.isEmpty() && !w2.isEmpty();
    }

    public String toString() {
        return decade + "," + w1 + "," + w2;
    }
}
