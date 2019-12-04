package com.jj.hive.video.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable, Text,NullWritable,Text> {

    private Text v = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String etl = ETLUtil.etl(value.toString());
        if(etl!=null){
            v.set(etl);
            context.write(NullWritable.get(),v);
        }

    }
}
