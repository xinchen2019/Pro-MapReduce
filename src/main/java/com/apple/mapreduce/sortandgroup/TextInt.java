package com.apple.mapreduce.sortandgroup;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: TextInt
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-02 13:53
 * @Version 1.1.0
 **/
public class TextInt implements WritableComparable<TextInt> {
    private String str;
    private int value;

    public String getStr() {
        return str;
    }

    public void setStr(String str) {
        this.str = str;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }


    public void readFields(DataInput in) throws IOException {
        str = in.readUTF();
        value = in.readInt();
    }


    public void write(DataOutput out) throws IOException {
        out.writeUTF(str);
        out.writeInt(value);
    }

    public int compareTo(TextInt o) {
        return o.getStr().compareTo(this.getStr());
    }
}
