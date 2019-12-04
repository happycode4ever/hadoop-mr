package com.jj.custom_outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class FileterRecordWriter extends RecordWriter<NullWritable, Text> {
    private FSDataOutputStream fos1 = null;
    private FSDataOutputStream fos2 = null;
    private FileSystem fs = null;

    public FileterRecordWriter(TaskAttemptContext job) {
        //初始化输出流
        try {
            Configuration conf = job.getConfiguration();
            fs = FileSystem.get(conf);
            fos1 = fs.create(new Path("H:\\bigdata-dev\\tools\\hadoop-data\\mr-customoutputformat\\result\\itstar.log"));
            fos2 = fs.create(new Path("H:\\bigdata-dev\\tools\\hadoop-data\\mr-customoutputformat\\result\\other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(NullWritable key, Text value) throws IOException, InterruptedException {
        String v = value.toString();
        String enter = "\n";
        v+=enter;
        if(v.contains("itstar")){
            fos1.write(v.getBytes());
        }else{
            fos2.write(v.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if(fos2!=null)fos2.close();
        if(fos1!=null)fos1.close();
        if(fs!=null)fs.close();
    }
}
