package com.dai.mapreduce.communitydetection.mapreduce1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class BlogDetection1Reducer extends Reducer<LongWritable, LongWritable, LongWritable, IntWritable> {
    private LongWritable outKey = new LongWritable();
    private IntWritable outValue = new IntWritable();

    private List<Long> list = new ArrayList<>();

    /**
     * 重写reduce方法，每个key都会运行一次reduce方法
     * @param key 输入的key
     * @param values 按key合并后的value的组合
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Reducer<LongWritable, LongWritable, LongWritable, IntWritable>.Context context) throws IOException, InterruptedException {
        for (LongWritable value : values) {
            // KEY的关注对象的组合
            list.add(value.get());
        }
        outKey.set(key.get());
        // 1 代表是共同关注者， 0 则不是共同关注者
        if (list.size() > 1) {
            outValue.set(1);
        } else {
            outValue.set(0);
        }
        // 写出输出数据到上下文
        context.write(outKey, outValue);
        list.clear();
    }
}
