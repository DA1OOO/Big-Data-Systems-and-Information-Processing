package com.dai.mapreduce.communitydetection.taskA.mapreduce3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 */
public class BlogDetection3Mapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    // 该map输出的key
    private LongWritable outKey = new LongWritable();
    // 该map输出的value
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
        // 获取一行
        String line = value.toString();
        String[] words = line.split("\t");
        String[] keys = words[0].split(",");
        outKey.set(Long.parseLong(keys[0]));
        outValue.set(keys[1] + "|" + words[1]);
        context.write(outKey, outValue);
        outKey.set(Long.parseLong(keys[1]));
        outValue.set(keys[0] + '|' + words[1]);
        context.write(outKey, outValue);
    }
}
