package com.jj.sequencefile_inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReadMapper extends Mapper<Text, BytesWritable,Text, NullWritable> {
    private Text k = new Text();
    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        String keyStr = key.toString();
        String valueStr = new String(value.getBytes());
        k.set("key:"+keyStr+",value:"+valueStr);
        context.write(k,NullWritable.get());
    }
}
