package com.apple.mapreduce.DistributedCache;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: DistributedCacheDriver
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-01 23:11
 * @Version 1.1.0
 **/
public class DistributedCacheDriver {
    public static void main(String[] args)
            throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

        args = new String[]{
                "data\\inputcache\\order.txt",
                "output\\inputcache"
        };

        //获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path(args[1]);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        //设置加载jar包路径
        job.setJarByClass(DistributedCacheDriver.class);

        //关联map
        job.setMapperClass(DistributedCacheMapper.class);

        //设置最终输出数据类型
        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(NullWritable.class);

        //设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //加载缓存数据

        job.addCacheFile(new URI("data/inputcache/pd.txt"));

        //map端join的逻辑不需要reduce阶段，设置reducetask数量为0
        job.setNumReduceTasks(0);

        //提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
