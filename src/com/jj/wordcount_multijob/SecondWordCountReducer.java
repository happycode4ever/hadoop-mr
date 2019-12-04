package com.jj.wordcount_multijob;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SecondWordCountReducer extends Reducer<Text,Text,Text,Text> {
    private Text infos = new Text();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer sb = new StringBuffer();
        for(Text v : values){
            sb.append(v.toString());
            sb.append("\t");
        }
        infos.set(sb.toString());
        context.write(key,infos);
    }
}
