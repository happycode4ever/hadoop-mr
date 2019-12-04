package com.jj.purge_counter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PurgeMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        boolean flag = parseLog(line,context);
       if(!flag)return;
       context.write(value,NullWritable.get());

    }

    private boolean parseLog(String line, Context context) {
        String[] fields = line.split(" ");
        if(fields.length<11) {
            context.getCounter("com.jj.purge_counter","PurgeMapper.parseLog.false").increment(1);
            return false;
        }else{
            context.getCounter("com.jj.purge_counter", "PurgeMapper.parseLog.true").increment(1);
            return true;
        }

    }
}
