package com.jj.findfriends_multijob;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SecondCountReducer extends Reducer<Text,Text,Text, NullWritable> {

    private Text k = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        for(Text t : values){
            sb.append(t.toString());
            sb.append(",");
        }
        String res = sb.substring(0, sb.length() - 1);
        k.set(key.toString()+":"+res);
        context.write(k,NullWritable.get());
    }
}
