package com.dai.mapreduce.communitydetection.mapreduce3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**

 */
public class BlogDetection3Reducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    private LongWritable outKey = new LongWritable();

    private Text outValue = new Text();

    private int maxCommonBlogNums = 0;

    private Long tempFlag = Long.valueOf(0);

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Reducer<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
//        String blogIndex = String.valueOf(key);
//        if (blogIndex.substring(blogIndex.length() - 4) != "2964") return;
        outKey.set(key.get());
        maxCommonBlogNums = 0;
        // 记录当前的A:B 中的B的大小
        tempFlag = Long.valueOf(0);
        for (Text value : values) {
            String[] words = value.toString().split("\\|");
            String[] commonBlog = words[1].split(", ");
            if (commonBlog.length > maxCommonBlogNums) {
                outValue.set(key + ":" + words[0] + "," + words[1] + "," + commonBlog.length);
                maxCommonBlogNums = commonBlog.length;
                tempFlag = Long.valueOf(words[0]);
            }
            if (commonBlog.length == maxCommonBlogNums) {
                if (Long.parseLong(words[0]) > tempFlag) {
                    outValue.set(key + ":" + words[0] + "," + words[1] + "," + commonBlog.length);
                    tempFlag = Long.valueOf(words[0]);
                }
            }
        }
        context.write(outKey, outValue);
    }
}
