package com.apple.mapreduce.sortandgroup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Program: Pro-MapReduce
 * @ClassName: SortPartitioner
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-02 13:58
 * @Version 1.1.0
 **/
public class SortPartitioner extends Partitioner<TextInt, IntWritable> {
    @Override
    public int getPartition(TextInt textInt, IntWritable intWritable, int numPartitions) {
        return textInt.getStr().hashCode() & Integer.MAX_VALUE % numPartitions;
    }
}
