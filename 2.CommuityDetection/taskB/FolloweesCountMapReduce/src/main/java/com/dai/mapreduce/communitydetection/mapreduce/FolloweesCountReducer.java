package com.dai.mapreduce.communitydetection.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**

 */
public class FolloweesCountReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {
    private LongWritable outKey = new LongWritable();

    private IntWritable outValue = new IntWritable();

    private int sum = 0;

    @Override
    protected void reduce(LongWritable key, Iterable<IntWritable> values, Reducer<LongWritable, IntWritable, LongWritable, IntWritable>.Context context) throws IOException, InterruptedException {
        sum = 0;
        for (IntWritable value : values) {
            sum += 1;
        }
        outKey.set(key.get());
        outValue.set(sum);
        context.write(outKey, outValue);
    }
}
