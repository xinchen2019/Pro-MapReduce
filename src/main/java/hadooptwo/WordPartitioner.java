package hadooptwo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Program: HadoopDemo
 * @ClassName: WordPartitioner
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-03-28 00:06
 * @Version 1.1.0
 **/
public class WordPartitioner extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable intWritable, int i) {
        //  获取单词key
        String firWord = key.toString().substring(0, 1);
        char[] charArray = firWord.toCharArray();
        int result = charArray[0];
        // int result = key.toString().charAt(0);
        //  根据奇数偶数分区
        if (result % 2 == 0) {
            return 0;
        } else {
            return 1;
        }
    }
}
