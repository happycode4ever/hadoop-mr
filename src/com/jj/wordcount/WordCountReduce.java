package com.jj.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //聚合map结果，输入是每个相同单词与数量的集合
        IntWritable valueOut = new IntWritable();
        int sum = 0;
        for(IntWritable i : values){
            int count = i.get();
            sum+=count;
        }
        //叠加的结果输出给context
        valueOut.set(sum);
        context.write(key,valueOut);
    }

}
