package com.dai.mapreduce.communitydetection.mapreduce3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class BlogDetection3Driver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        // 1. 获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2. 设置jar包路径
        job.setJarByClass(BlogDetection3Driver.class);

        // 3. 关联 mapper 和 reducer
        job.setMapperClass(BlogDetection3Mapper.class);
        job.setReducerClass(BlogDetection3Reducer.class);

        // 4. 设置 mapper 输出的KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 5. 设置最终输出的KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 读取文件进入缓存
        job.addCacheFile(new URI("file:///C://Users//Administrator//Desktop//Big-Data-Systems-and-Information-Processing//2.CommuityDetection//output0//part-r-00000"));

        // 6. 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\Administrator\\Desktop\\Big-Data-Systems-and-Information-Processing\\2.CommuityDetection\\output1\\part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\Administrator\\Desktop\\Big-Data-Systems-and-Information-Processing\\2.CommuityDetection\\output2"));
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7. 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
