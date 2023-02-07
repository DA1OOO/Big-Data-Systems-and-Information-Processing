package com.dai.mapreduce.communitydetection.mapreduce2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**

 */
public class BlogDetection2Reducer extends Reducer<IntWritable, IntWritable, Text, LongWritable> {

    private Text outKey = new Text();

    private LongWritable outValue = new LongWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Reducer<IntWritable, IntWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        outKey.set("Community " + key);
        outValue.set(sum);
        context.write(outKey, outValue);
    }
}
