package com.dai.mapreduce.communitydetection.taskA.mapreduce2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * 接受MapReduce1输出的数据
 * 输出K - A,B | V - C 表示A,B共同关注了C
 */
public class BlogDetection2Mapper extends Mapper<LongWritable, Text, Text, Text> {
    // 该map输出的key
    private Text outKey = new Text();
    // 该map输出的value
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        // 获取一行
        // 2	[49, 54, 55, 74, 89]
        String line = value.toString();
        String[] words = line.split("\t");
        // outValue 由 关注对象组成
        outValue.set(words[0]);
        // 将 [49, 54, 55, 74, 89] 处理到数组中
        String[] followees = words[1].split(", ");
        followees[0] = followees[0].substring(1);
        followees[followees.length - 1] = followees[followees.length - 1].substring(0, followees[followees.length - 1].length() - 1);
        // 求出 [49, 54, 55, 74, 89] 的笛卡尔积
        for (int i = 0; i < followees.length; ++i) {
            for (int j = i + 1; j < followees.length; ++j) {
                outKey.set(followees[i] + "," + followees[j]);
                context.write(outKey, outValue);
            }
        }
    }
}
