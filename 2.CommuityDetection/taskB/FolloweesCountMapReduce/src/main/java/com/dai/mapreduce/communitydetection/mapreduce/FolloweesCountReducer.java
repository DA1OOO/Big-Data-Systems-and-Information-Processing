package com.dai.mapreduce.communitydetection.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class FolloweesCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Text outKey = new Text();

    private IntWritable outValue = new IntWritable();

    private int sum = 0;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        sum = 0;
        for (IntWritable value : values) {
            sum += 1;
        }
        outKey.set(key);
        outValue.set(sum);
        context.write(outKey, outValue);
    }
}
