package com.apple.mapreduce.sortandgroup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: SortMapper
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-02 13:52
 * @Version 1.1.0
 */
public class SortMapper extends Mapper<Object, Text, TextInt, IntWritable> {

    private TextInt textInt = new TextInt();

    private IntWritable intWritable = new IntWritable(0);

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        int i = Integer.parseInt(value.toString());


        System.out.println("key: " + key.toString());
        System.out.println("value: " + value.toString());

        textInt.setStr(key.toString());
        textInt.setValue(i);
        intWritable.set(i);

        context.write(textInt, intWritable);
    }
}
