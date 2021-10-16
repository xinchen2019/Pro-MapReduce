package com.apple.mapreduce.friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: OneShareFriendsMapper
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-03 10:55
 * @Version 1.1.0
 **/
public class OneShareFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //获取一行
        String line = value.toString();

        //切割
        String[] fields = line.split(":");

        //获取person和好友
        String person = fields[0];
        String[] friends = fields[1].split(",");

        for (String friend : friends) {
            context.write(new Text(friend), new Text(person));
        }

    }
}
