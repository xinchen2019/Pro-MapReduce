package com.apple.mapreduce.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: SequenceFileReader
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-04-11 16:12
 * @Version 1.1.0
 **/
public class SequenceFileReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context)
            throws IOException, InterruptedException {
        for (BytesWritable bytesWritable : values) {
            context.write(key, bytesWritable);
        }
    }
}
