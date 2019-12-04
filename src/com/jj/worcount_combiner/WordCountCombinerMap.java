package com.jj.worcount_combiner;

import com.google.common.base.Splitter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountCombinerMap extends Mapper<LongWritable,Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable keyIn, Text valueIn, Context context) throws IOException, InterruptedException {
        Text keyOut = new Text();
        IntWritable valueOut = new IntWritable(1);
        //切分单词
        String value = valueIn.toString();
        Iterable<String> it = Splitter.on(" ").split(value);
        for(String s : it){
            keyOut.set(s);
            //每个切分的单词数量是1输出给Reduce
            context.write(keyOut,valueOut);
        }
    }
}
