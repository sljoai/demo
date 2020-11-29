package com.song.cn.hadoop.mr.wordcount.flatsum;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NumberBean implements Writable {

    private int first;

    private int second;

    private float third;

    public NumberBean() {
        super();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
        out.writeFloat(third);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
        third = in.readFloat();
    }

    @Override
    public String toString() {
        return first + "\t" +
                second + "\t" +
                third
                ;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    public float getThird() {
        return third;
    }

    public void setThird(float third) {
        this.third = third;
    }
}
