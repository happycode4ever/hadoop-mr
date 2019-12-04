package com.jj.cumstom_inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class CustomMergeRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private FileSplit fileSplit = null;
    private Configuration conf = null;
    private BytesWritable value = new BytesWritable();
    private boolean processed = false;
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        fileSplit = (FileSplit) split;
        conf = context.getConfiguration();
    }
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!processed){
            FileSystem fs = null;
            FSDataInputStream fis = null;
            try {
                //设定缓存就是文件大小
                byte[] buf = new byte[(int) fileSplit.getLength()];
                //切片获取路径，就是路径+文件名
                Path path = fileSplit.getPath();
                //路径反取文件系统
                fs = path.getFileSystem(conf);
                //文件系统获取输入流
                fis = fs.open(path);
                //读取文件到缓冲区
                IOUtils.readFully(fis,buf,0,buf.length);
                //设定写出value就是缓冲区内容
                value.set(buf,0,buf.length);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if(fis!=null)IOUtils.closeStream(fis);
                if(fs!=null)IOUtils.closeStream(fs);
            }
            //读完文件进度完成
            processed = true;
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return processed?1f:0f;
    }

    @Override
    public void close() throws IOException {

    }
}
