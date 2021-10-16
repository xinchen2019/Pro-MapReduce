package com.apple.mapreduce.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: TwoIndexReducer
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-03 10:28
 * @Version 1.1.0
 **/
public class TwoIndexReducer extends Reducer<Text, Text, Text, Text> {


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        //拼接
        for (Text value : values) {
            sb.append(value.toString().replace("\t", "-->") + "\t");
        }
        //写出
        context.write(key, new Text(sb.toString()));
    }
}
