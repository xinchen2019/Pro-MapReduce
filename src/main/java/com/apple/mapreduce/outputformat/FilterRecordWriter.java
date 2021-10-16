package com.apple.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: FileRecordWriter
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-04-11 17:03
 * @Version 1.1.0
 **/
public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream appleOut = null;
    FSDataOutputStream otherOut = null;


    String appleOutPath = "output/outputStream/apple/apple.txt";
    String otherOutPath = "output/outputStream/apple/other.txt";


    public FilterRecordWriter(TaskAttemptContext job) {
        //1 获取文件系统
        FileSystem fs;
        try {
            //2 获取文件系统
            fs = FileSystem.get(job.getConfiguration());
            //3 创建两个输出
            appleOut = fs.create(new Path(appleOutPath));
            otherOut = fs.create(new Path(otherOutPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value)
            throws IOException, InterruptedException {
        //判断是否包含apple
        if (key.toString().contains("apple")) {
            appleOut.write(key.getBytes());
        } else {
            otherOut.write(key.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context)
            throws IOException, InterruptedException {
        //关闭资源
        IOUtils.closeStream(appleOut);
        IOUtils.closeStream(otherOut);
    }
}

