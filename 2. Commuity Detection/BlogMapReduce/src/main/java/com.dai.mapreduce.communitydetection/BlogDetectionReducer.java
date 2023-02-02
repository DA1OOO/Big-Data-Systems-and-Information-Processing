package com.dai.mapreduce.communitydetection;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * reducer 层接受 mapper 的 OUT
 * KEYIN 和 VALUEIN 与 mapper的 OUT 保持一致
 * IN: 每对图关系 A->B
 * output 格式:s A:B, {C,E}, 2
 */
public class BlogDetectionReducer extends Reducer<LongWritable, LongWritable, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    /**
     * 重写reduce方法，每个key都会运行一次reduce方法
     * @param key 输入的key
     * @param values 按key合并后的value的组合
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Reducer<LongWritable, LongWritable, Text, Text>.Context context) throws IOException, InterruptedException {
        List<LongWritable> list = new ArrayList<LongWritable>();
        for (LongWritable value : values) {
            list.add(value);
        }
        // 写出输出数据到上下文
        context.write(outKey, outValue);
    }
}
