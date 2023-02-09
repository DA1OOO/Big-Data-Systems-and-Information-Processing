package com.dai.mapreduce.communitydetection.mapreduce2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class BlogDetection2Driver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. 获取job
        Configuration conf = new Configuration();
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
      FileInputFormat.setInputPaths(job, new Path("C:\\Users\\Administrator\\Desktop\\Big-Data-Systems-and-Information-Processing\\2.CommuityDetection\\output1\\part-r-00000"));
      FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\Administrator\\Desktop\\Big-Data-Systems-and-Information-Processing\\2.CommuityDetection\\output2"));
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7. 提交job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}