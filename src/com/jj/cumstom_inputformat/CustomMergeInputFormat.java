package com.jj.cumstom_inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class CustomMergeInputFormat extends FileInputFormat<NullWritable, BytesWritable> {
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        //合并文件不需要切割
        return false;
    }

    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        CustomMergeRecordReader customMergeRecordReader = new CustomMergeRecordReader();
        customMergeRecordReader.initialize(split,context);
        return customMergeRecordReader;
    }
}
