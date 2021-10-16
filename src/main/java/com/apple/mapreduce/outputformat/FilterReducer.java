package com.apple.mapreduce.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: FilterReducer
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-04-11 17:31
 * @Version 1.1.0
 **/
public class FilterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {

        String k = key.toString();
        k = k + "\r\n";
        context.write(new Text(k), NullWritable.get());
    }
}
