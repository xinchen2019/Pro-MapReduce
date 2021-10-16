package com.apple.mapreduce.keyvaletextinputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: KVTextReducer
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-03 15:32
 * @Version 1.1.0
 **/
public class KVTextReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    LongWritable v = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        long count = 0L;

        //汇总统计
        for (LongWritable value : values) {
            count += value.get();
        }

        v.set(count);

        context.write(key, v);

    }
}
