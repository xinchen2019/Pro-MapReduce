package com.apple.mapreduce.friends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * @Program: Pro-MapReduce
 * @ClassName: OneShareFriendsReducer
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-03 11:00
 * @Version 1.1.0
 **/
public class OneShareFriendsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();

        //拼接
        for (Text person : values) {
            sb.append(person).append(",");
        }

        //写出
        context.write(key, new Text(sb.toString()));
    }
}
