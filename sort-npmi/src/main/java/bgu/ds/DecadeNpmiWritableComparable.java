package bgu.ds;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DecadeNpmiWritableComparable implements WritableComparable<DecadeNpmiWritableComparable> {
    private int decade;
    private double npmi;

    public DecadeNpmiWritableComparable() {
        decade = 0;
        npmi = 0;
    }

    public DecadeNpmiWritableComparable(int decade, double npmi) {
        this.decade = decade;
        this.npmi = npmi;
    }

    @Override
    public int compareTo(DecadeNpmiWritableComparable o) {
        if (decade != o.decade) {
            return Integer.compare(decade, o.decade);
        }
        return Double.compare(npmi, o.npmi);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(decade);
        dataOutput.writeDouble(npmi);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        decade = dataInput.readInt();
        npmi = dataInput.readDouble();
    }

    public int getDecade() {
        return decade;
    }

    public double getNpmi() {
        return npmi;
    }
}
