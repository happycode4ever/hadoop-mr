package com.jj.custom_keyvaluetextinputformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KeyValueMapper extends Mapper<Text,Text,Text, IntWritable> {

    private IntWritable v = new IntWritable();
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("com.jj.custom_keyvaluetextinputformat.KeyValueMapper.map key:"+key.toString()+",value:"+value.toString());
        v.set(1);
        context.write(key,v);
    }
}
