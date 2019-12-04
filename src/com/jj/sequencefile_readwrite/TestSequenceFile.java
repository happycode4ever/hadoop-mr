package com.jj.sequencefile_readwrite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class TestSequenceFile {
    public static void main(String[] args) throws IOException {
        // // TODO Auto-generated method stub
        Configuration conf = new Configuration();
        Path seqFile = new Path("/test/seqFile2.seq");
        // Writer内部类用于文件的写操作,假设Key和Value都为Text类型
        SequenceFile.Writer writer = SequenceFile.createWriter(
                // 配置
                conf,
                // 文件位置
                SequenceFile.Writer.file(seqFile),
                // 顺序文件K
                SequenceFile.Writer.keyClass(Text.class),
                // 顺序文件V
                SequenceFile.Writer.valueClass(Text.class),
                // 是否压缩
                SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE)
        );


        // 通过writer向文档中写入记录
        writer.append(new Text("key"), new Text("value"));


        IOUtils.closeStream(writer);// 关闭write流


        // 通过reader从文档中读取记录
        SequenceFile.Reader reader = new SequenceFile.Reader(
                // 配置
                conf,
                // 读文件
                SequenceFile.Reader.file(seqFile)
        );
        Text key = new Text();
        Text value = new Text();
        // 将文件读出来，打印
        while (reader.next(key, value)) {
            System.out.println(key);
            System.out.println(value);
        }
        IOUtils.closeStream(reader);// 关闭read流
    }
}
