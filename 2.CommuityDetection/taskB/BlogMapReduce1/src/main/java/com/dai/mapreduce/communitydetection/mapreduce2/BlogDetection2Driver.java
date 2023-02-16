package com.dai.mapreduce.communitydetection.mapreduce2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class BlogDetection2Driver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. 获取job
        Configuration conf = new Configuration();
        // 设置切片大小参数
        //splitSize=64*1024*1024
        conf.set(FileInputFormat.SPLIT_MAXSIZE,"67108864");
        //splitSize=256*1024*1024
//        conf.set(FileInputFormat.SPLIT_MAXSIZE,"268435456");

        // 开启map输出
        conf.setBoolean("mapreduce.map.output.compress", true);
        // 一旦map端开启输出，我们就要指定map压缩采用什么压缩机制
        conf.setClass("mapreduce.map.output.compress.codec", DefaultCodec.class, CompressionCodec.class);



        Job job = Job.getInstance(conf);

        // 2. 设置jar包路径
        job.setJarByClass(BlogDetection2Driver.class);

        // 3. 关联 mapper 和 reducer
        job.setMapperClass(BlogDetection2Mapper.class);
        job.setReducerClass(BlogDetection2Reducer.class);

        // 4. 设置 mapper 输出的KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 5. 设置最终输出的KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 6. 设置输入输出路径
//      FileInputFormat.setInputPaths(job, new Path("C:\\Users\\Administrator\\Desktop\\Big-Data-Systems-and-Information-Processing\\2.CommuityDetection\\output1\\part-r-00000"));
//      FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\Administrator\\Desktop\\Big-Data-Systems-and-Information-Processing\\2.CommuityDetection\\output2"));
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7. 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
