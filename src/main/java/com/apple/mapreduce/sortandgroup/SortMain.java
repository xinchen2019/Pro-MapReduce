package com.apple.mapreduce.sortandgroup;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: SortMain
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-02 18:26
 * @Version 1.1.0
 * <p>
 * 参考链接 https://blog.csdn.net/syraphie/article/details/22691613?utm_medium=distribute.pc_relevant_t0.none-task-blog-2%7Edefault%7ECTRLIST%7Edefault-1.no_search_link&depth_1-utm_source=distribute.pc_relevant_t0.none-task-blog-2%7Edefault%7ECTRLIST%7Edefault-1.no_search_link
 **/
public class SortMain {
    public static void main(String[] args) throws IOException {

        args = new String[]{
                "data\\sortandgroup\\word.txt",
                "output\\sortandgroup"
        };

        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage:sort<int><out>");
            System.exit(2);
        }

        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path(args[1]);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        Job job = new Job(conf, "sort");
        job.setJarByClass(SortMain.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapperClass(SortMapper.class);
        job.setPartitionerClass(SortPartitioner.class);
        job.setMapOutputKeyClass(TextInt.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setSortComparatorClass(TextIntComparator.class);
        job.setGroupingComparatorClass(TextComparator.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        try {
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
