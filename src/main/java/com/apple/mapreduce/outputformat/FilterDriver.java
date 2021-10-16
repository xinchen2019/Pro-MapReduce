package com.apple.mapreduce.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: FilterDriver
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-04-11 17:33
 * @Version 1.1.0
 **/
public class FilterDriver {

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{
                "data\\outputStream\\log.txt",
                "output\\"
        };

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FilterDriver.class);
        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //要将自定义的输出格式组件设置到job中
        job.setOutputFormatClass(FilterOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        /**
         *  虽然我们自定义了outputformat，但是因为我们的outputformat继承自fileoutputformat
         *  而fileoutputformat要输出一个_SUCCESS文件，所以，在这还得指定一个输出目录
         */
        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path(args[1]);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
