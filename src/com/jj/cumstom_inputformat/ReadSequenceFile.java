package com.jj.cumstom_inputformat;

import org.apache.el.util.ReflectionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

public class ReadSequenceFile {
    public static void read1() throws IOException {
        String path ="H:\\bigdata-dev\\tools\\hadoop-data\\mr-custominputformat\\output\\part-r-00000";
        Configuration conf = new Configuration();
        // 通过reader从文档中读取记录
        SequenceFile.Reader reader = new SequenceFile.Reader(
                // 配置
                conf,
                // 读文件
                SequenceFile.Reader.file(new Path(path)));

//        Text key = new Text();
//        BytesWritable value = new BytesWritable();
        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
        // 将文件读出来，打印
        while (reader.next(key, value)) {
            System.out.println(key);
            System.out.println(value);
        }
        IOUtils.closeStream(reader);// 关闭read流
    }

    public static void main(String[] args) throws IOException {
        read1();
    }
}
