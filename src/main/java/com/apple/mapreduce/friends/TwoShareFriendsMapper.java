package com.apple.mapreduce.friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @Program: Pro-MapReduce
 * @ClassName: TwoShareFriendsMapper
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-03 11:54
 * @Version 1.1.0
 **/
public class TwoShareFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        String[] friends_persons = line.split("\t");

        String friend = friends_persons[0];
        String[] persons = friends_persons[1].split(",");

        Arrays.sort(persons);

        for (int i = 0; i < persons.length - 1; i++) {
            for (int j = i + 1; j < persons.length; j++) {
                context.write(new Text(persons[i] + "-" + persons[j]), new Text(friend));
            }
        }
    }
}
