package com.dai.mapreduce.communitydetection;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN: map阶段的输入的key的类型，每次读取的偏移量 LongWritable
 * VALUEIN: map阶段输入的value
 * KEYOUT: map阶段输出的key
 * VALUEOUT: may阶段输出的value
 */
public class BlogDetectionMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }
}
