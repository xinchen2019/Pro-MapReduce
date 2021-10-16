package com.apple.mapreduce.flowsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * @Program: Pro-MapReduce
 * @ClassName: FlowCountSortDriver
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-01 11:52
 * @Version 1.1.0
 **/
public class FlowCountSortDriver {

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        String inputpath = "data\\flowsort\\flowsort_phone_data.txt";
        String outputpath = "output\\flowsort\\flowsort_phone_data";

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowCountSortDriver.class);

        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path(inputpath));

        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path(outputpath);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        FileOutputFormat.setOutputPath(job, new Path(outputpath));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
