package com.apple.mapreduce.sortandgroup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Program: Pro-MapReduce
 * @ClassName: SortReducer
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-02 14:00
 * @Version 1.1.0
 **/
public class SortReducer extends Reducer<TextInt, IntWritable, Text, Text> {

    @Override
    protected void reduce(TextInt key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        StringBuffer sb = new StringBuffer();
        Iterator<IntWritable> it = values.iterator();
        while (it.hasNext()) {
            int value = it.next().get();
            sb.append(value + ",");
        }
        int length = sb.length();
        if (length > 0) {
            sb.deleteCharAt(length - 1);
        }
        context.write(new Text(key.getStr()), new Text(sb.toString()));
    }
}
