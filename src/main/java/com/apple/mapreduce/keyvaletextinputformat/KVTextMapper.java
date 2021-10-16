package com.apple.mapreduce.keyvaletextinputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: KVTextMapper
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-03 15:30
 * @Version 1.1.0
 **/
public class KVTextMapper extends Mapper<Text, Text, Text, LongWritable> {

    Text k = new Text();
    LongWritable v = new LongWritable();

    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        k.set(key);
        v.set(1);
        //写出

        context.write(k, v);
    }
}
