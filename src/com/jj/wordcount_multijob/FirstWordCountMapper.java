package com.jj.wordcount_multijob;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class FirstWordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    private String fileName = null;
    private Text k = new Text();
    private IntWritable v = new IntWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(" ");
        for(String word : fields){
            //写给reduce格式 key\001filename count
            k.set(word+"\001"+fileName);
            v.set(1);
            context.write(k,v);
        }
    }
}
