package com.apple.mapreduce.compress;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: WordCountReducer
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-03 15:05
 * @Version 1.1.0
 **/
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int count = 0;

        //汇总
        for (IntWritable value : values) {
            count += value.get();
        }
        //输出
        context.write(key, new IntWritable(count));
    }
}
