package com.apple.mapreduce.wordcount2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Program: Pro-MapReduce
 * @ClassName: wordcount2
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-10-02 13:02
 * @Version 1.1.0
 * 参考链接：https://www.it610.com/article/416398.htm
 */
public class Wordcount2Job extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        args = new String[]{

        };
        int exitCode = ToolRunner.run(new Wordcount2Job(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        System.out.println("this is the host default name =" + conf.get("fs.default.name"));

        JobConf job = new JobConf(conf, Wordcount2Job.class);
        job.setJarByClass(Wordcount2Job.class);

        Path in = new Path(args[1]);
        Path out = new Path(args[2]);

        FileInputFormat.setInputPaths(job, in);

        //Mapper
        //job.setMapperClass();

        FileInputFormat.setInputPaths(job, out);


        //Job job = Job.getInstance(conf);


        // 设置job的各种详细参数
        job.setJobName("my-app");
//        job.setInputFormatClass();
//        job.setInputPath(in);
//        job.setOutputPath(out);
        //job.setMapperClass(MyMapper.class);
        //job.setReducerClass(MyReducer.class);

        //提交job
        //JobClient.runJob(job);

        return 0;
    }
}
