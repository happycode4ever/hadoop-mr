package com.jj.wordcount_multijob;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FirstWordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    private IntWritable v = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable i : values){
            sum+=i.get();
        }
        v.set(sum);
        context.write(key,v);
    }
}
