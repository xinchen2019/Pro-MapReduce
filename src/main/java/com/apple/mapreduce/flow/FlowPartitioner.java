package com.apple.mapreduce.flow;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Program: Pro-MapReduce
 * @ClassName: FlowPartitioner
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-03-28 00:59
 * @Version 1.1.0
 **/
public class FlowPartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text key, FlowBean value, int numPartitions) {
        // 1 需求：根据电话号码的前3位是几来分区
        // 拿到电话号码的前3位
        String phoneNum = key.toString().substring(0, 3);
        int partitions = 4;
        if ("135".equals(phoneNum)) {
            partitions = 0;
        } else if ("136".equals(phoneNum)) {
            partitions = 1;
        } else if ("137".equals(phoneNum)) {
            partitions = 2;
        } else if ("138".equals(phoneNum)) {
            partitions = 3;
        }
        return partitions;
    }
}
