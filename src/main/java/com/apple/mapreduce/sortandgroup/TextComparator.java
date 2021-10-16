package com.apple.mapreduce.sortandgroup;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Program: Pro-MapReduce
 * @ClassName: TextComparator
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-02 14:07
 * @Version 1.1.0
 **/
public class TextComparator extends WritableComparator {

    public TextComparator() {
        super(TextInt.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TextInt o1 = (TextInt) a;
        TextInt o2 = (TextInt) b;
        return o1.getStr().compareTo(o2.getStr());
    }
}
