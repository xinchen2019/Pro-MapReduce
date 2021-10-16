package com.apple.mapreduce.sortandgroup;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Program: Pro-MapReduce
 * @ClassName: TextIntComparator
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-02 14:03
 * @Version 1.1.0
 **/
public class TextIntComparator extends WritableComparator {

    public TextIntComparator() {
        super(TextInt.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TextInt o1 = (TextInt) a;
        TextInt o2 = (TextInt) b;

        if (!o1.getStr().equals(o2.getStr())) {
            return o1.getStr().compareTo(o2.getStr());
        } else {
            return o1.getValue() - o2.getValue();
        }
    }
}
