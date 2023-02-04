package com.dai.mapreduce.communitydetection.mapreduce1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


public class BlogDetection1Mapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
    // 该map输出的key
    private LongWritable outKey = new LongWritable();
    // 该map输出的value
    private LongWritable outValue = new LongWritable();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, LongWritable>.Context context) throws IOException, InterruptedException {
        // 获取一行
        String line = value.toString();
        // 用空格将一行中的两个数据进行切割 [A，B] 表示B follow A
        String[] words = line.split(" ");
        // 被关注者作为key
        outKey.set(Long.parseLong(words[0]));
        // 关注者作为value
        outValue.set(Long.parseLong(words[1]));
        // 将数据分割为 K-V 键值对
        context.write(outKey, outValue);
    }
}
