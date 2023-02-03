package com.dai.mapreduce.communitydetection.taskA.mapreduce2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**

 */
public class BlogDetection2Reducer extends Reducer<Text, Text, Text, Text> {
    private Text outKey = new Text();

    private Text outValue = new Text();

    private List<String> list = new ArrayList<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        outKey.set(key);
        for (Text value : values) {
            list.add(value.toString());
        }
        outValue.set(list.toString());
        context.write(outKey, outValue);
        list.clear();
    }
}
