package com.apple.mapreduce.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: OneIndexReducer
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-03 10:01
 * @Version 1.1.0
 **/
public class OneIndexReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        //累加求和
        for (IntWritable value : values) {
            count += value.get();
        }
        //写出
        context.write(key, new IntWritable(count));
    }
}
