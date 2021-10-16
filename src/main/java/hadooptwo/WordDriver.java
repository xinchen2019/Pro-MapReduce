package hadooptwo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Program: HadoopDemo
 * @ClassName: WordDriver
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-03-28 00:02
 * @Version 1.1.0
 **/
public class WordDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException,
            InterruptedException {

        // 获取配置信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        // 设置 jar 加载路径
        job.setJarByClass(WordDriver.class);
        // 设置 map 和 Reduce 类
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class);

        // 设置 map 输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 设置 Reduce 输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 设置输入和输出路径

        String inputPath = "file:///C:\\Users\\13128\\Desktop\\1.txt";
        String outputPath = "file:///C:\\Users\\13128\\Desktop\\2.txt";
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        //  设置分区
        job.setPartitionerClass(WordPartitioner.class);
        // 设置任务
        job.setNumReduceTasks(2);
        job.setCombinerClass(WordCombiner.class);
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304 * 1024);
        CombineTextInputFormat.setMinInputSplitSize(job, 2097152 * 1024);
        //  提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
