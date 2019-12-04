package com.jj.cumstom_inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class CustomMergerMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
    private Text text = new Text();

//    @Override
//    protected void setup(Context context) throws IOException, InterruptedException {
//       FileSplit fileSplit = (FileSplit) context.getInputSplit();
//        Path path = fileSplit.getPath();
//        text.set(path.toString());
//    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        Path path = fileSplit.getPath();
        text.set(path.toString());
        context.write(text,value);
    }
}
