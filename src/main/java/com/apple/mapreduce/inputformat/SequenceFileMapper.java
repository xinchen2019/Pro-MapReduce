package com.apple.mapreduce.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


/**
 * @Program: Pro-MapReduce
 * @ClassName: SequenceFileMapper
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-04-11 15:54
 * @Version 1.1.0
 **/
public class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
    Text k = new Text();

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        //获取文件的路径和名称
        FileSplit split = (FileSplit) context.getInputSplit();

        Path path = split.getPath();

        k.set(path.toString());
    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context)
            throws IOException, InterruptedException {

        context.write(k, value);

    }
}
