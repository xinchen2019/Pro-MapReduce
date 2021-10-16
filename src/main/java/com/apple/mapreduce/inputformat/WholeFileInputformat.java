package com.apple.mapreduce.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @Program: Pro-MapReduce
 * @ClassName: WhpleFileInputformat
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-04-11 15:17
 * @Version 1.1.0
 **/
public class WholeFileInputformat extends FileInputFormat<NullWritable, BytesWritable> {

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(
            InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        //创建对象
        WholeRecordReader recordReader = new WholeRecordReader();

        recordReader.initialize(split, context);
        return recordReader;
    }
}
