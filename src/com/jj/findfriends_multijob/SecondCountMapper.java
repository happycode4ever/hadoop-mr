package com.jj.findfriends_multijob;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondCountMapper extends Mapper<LongWritable, Text,Text, Text> {
    private Text k = new Text();
    private Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(":");
        String user = fields[0];
        String friends = fields[1];
        String[] friendsArray = friends.split(",");
        for(int i=0;i<friendsArray.length-1;i++){
            for(int j=i+1;j<friendsArray.length;j++){
                k.set(friendsArray[i]+"-"+friendsArray[j]);
                v.set(user);
                context.write(k,v);
            }
        }
    }
}
