package hadooptwo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Program: HadoopDemo
 * @ClassName: WordcountReducer
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-03-27 23:59
 * @Version 1.1.0
 **/
public class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> value, Context context)
            throws IOException, InterruptedException {
        //  累加求和
        int sum = 0;
        for (IntWritable count : value) {
            sum += count.get();
            //  输出
            context.write(key, new IntWritable(sum));
        }
    }
}
