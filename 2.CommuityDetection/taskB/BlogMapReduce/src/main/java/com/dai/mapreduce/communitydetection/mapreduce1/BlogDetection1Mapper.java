package com.dai.mapreduce.communitydetection.mapreduce1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * Task a: 寻找两篇博客的共同关注
 * Input: _relation
 * KEYIN: map阶段的输入的key的类型，默认为每次读取的偏移量 LongWritable
 * VALUEIN: map阶段输入的value Text: 默认为读取一整行的内容
 * KEYOUT: map阶段输出的key
 * VALUEOUT: may阶段输出的value
 */
public class BlogDetection1Mapper extends Mapper<LongWritable, Text, Text, Text> {
    // 该map输出的key
    private Text outKey = new Text();
    // 该map输出的value
    private Text outValue = new Text();
    /**
     * map 阶段, map每行都会被调用一次
     * @param key 输入时的key
     * @param value 输入时的value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        // 获取一行
        String line = value.toString();
        // 用空格将一行中的两个数据进行切割 [A，B] 表示B follow A
        String[] words = line.split(" ");
        // 被关注者作为key
        outKey.set(words[0]);
        // 关注者作为value
        outValue.set(words[1]);
        // 将数据分割为 K-V 键值对
        context.write(outKey, outValue);
    }
}
