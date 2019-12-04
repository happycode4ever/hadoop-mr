package com.jj.custom_outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FilterOutputFormat extends FileOutputFormat<NullWritable, Text> {
    @Override
    public RecordWriter<NullWritable, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        FileterRecordWriter fileterRecordWriter = new FileterRecordWriter(job);
        return fileterRecordWriter;
    }
}
