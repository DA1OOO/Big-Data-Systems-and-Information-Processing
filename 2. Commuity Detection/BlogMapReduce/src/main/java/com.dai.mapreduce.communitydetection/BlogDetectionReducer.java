package com.dai.mapreduce.communitydetection;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reducer 层接受 mapper 的 OUT
 * KEYIN 和 VALUEIN 与 mapper的 OUT 保持一致
 * IN: 每对图关系 A->B
 * output 格式:s A:B, {C,E}, 2
 */
public class BlogDetectionReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Reducer<LongWritable, LongWritable, LongWritable, LongWritable>.Context context) throws IOException, InterruptedException {

    }
}
