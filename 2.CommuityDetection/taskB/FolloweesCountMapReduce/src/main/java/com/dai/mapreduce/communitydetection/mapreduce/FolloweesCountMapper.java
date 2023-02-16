package com.dai.mapreduce.communitydetection.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 */
public class FolloweesCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    // 该map输出的key
    private Text outKey = new Text();
    // 该map输出的value
    private IntWritable outValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 获取一行
        String line = value.toString();
        String[] words = line.split(" ");
        outKey.set(words[1]);
        context.write(outKey, outValue);
    }
}
