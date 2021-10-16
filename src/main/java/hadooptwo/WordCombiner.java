package hadooptwo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Program: HadoopDemo
 * @ClassName: WordCombiner
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-03-28 00:19
 * @Version 1.1.0
 **/
public class WordCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {
        // 计算累加和
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }
}
