package com.jj.wordcount_multijob;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondWordCountMapper extends Mapper<LongWritable, Text,Text,Text> {
    private Text k = new Text();
    private Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\001");
        String word = fields[0];
        String info = fields[1];
        //替换成单词是key
        k.set(word);
        //改造输出样式 file-->num
        info = info.replace("\t","-->");
        v.set(info);
        context.write(k,v);
    }
}
