package bgu.ds.common.mapreduce;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TaggedValue implements Writable {
    private TagEnum tag;
    private long value;

    public TaggedValue() {
        this.tag = null;
        this.value = 0;
    }

    public TaggedValue(TagEnum tag, long value) {
        this.tag = tag;
        this.value = value;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(tag.ordinal());
        dataOutput.writeLong(value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        tag = TagEnum.values()[dataInput.readInt()];
        value = dataInput.readLong();
    }

    public TagEnum getTag() {
        return tag;
    }

    public long getValue() {
        return value;
    }

    public String toString() {
        return tag + " " + value;
    }
}
