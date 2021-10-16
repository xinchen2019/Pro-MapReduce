package com.apple.mapreduce.friends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: TwoShareFriendsReducer
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-03 12:01
 * @Version 1.1.0
 **/
public class TwoShareFriendsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        StringBuffer sb = new StringBuffer();

        for (Text friend : values) {
            sb.append(friend).append(" ");
        }
        context.write(key, new Text(sb.toString()));
    }
}
