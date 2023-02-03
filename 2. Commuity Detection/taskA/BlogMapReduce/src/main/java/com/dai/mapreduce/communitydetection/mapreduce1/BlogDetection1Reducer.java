package com.dai.mapreduce.communitydetection.mapreduce1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * reducer 层接受 mapper 的 OUT
 * KEYIN 和 VALUEIN 与 mapper的 OUT 保持一致
 * IN: 每对图关系 A->B
 * output 格式:    K - A : V - B,C,D (A : A的粉丝)
 */
public class BlogDetection1Reducer extends Reducer<LongWritable, LongWritable, LongWritable, Text> {
    private LongWritable outKey = new LongWritable();
    private Text outValue = new Text();
    private Set<Long> set = new HashSet<Long>();

    /**
     * 重写reduce方法，每个key都会运行一次reduce方法
     * @param key 输入的key
     * @param values 按key合并后的value的组合
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Reducer<LongWritable, LongWritable, LongWritable, Text>.Context context) throws IOException, InterruptedException {
        for (LongWritable value : values) {
            // KEY的关注对象的组合
            set.add(value.get());
        }
        outKey.set(key.get());
        outValue.set(set.toString());
        set.clear();
        // 写出输出数据到上下文
        context.write(outKey, outValue);
    }
}
