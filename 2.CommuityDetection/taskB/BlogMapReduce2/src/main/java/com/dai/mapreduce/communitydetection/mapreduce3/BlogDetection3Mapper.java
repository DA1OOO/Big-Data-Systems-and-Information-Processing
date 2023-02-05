package com.dai.mapreduce.communitydetection.mapreduce3;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


public class BlogDetection3Mapper extends Mapper<LongWritable, Text, Text, Text> {
    // 该map输出的key
    private Text outKey = new Text();
    // 该map输出的value
    private Text outValue = new Text();

    private Map<Long, Integer> followeesCountMap = new HashMap<>();

    // mapper前初始化加载缓存文件
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        //通过缓存文件得到小表数据 output0
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);

        //获取文件系统对象,并开流
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(path);

        //通过包装流转换为 reader,方便按行读取
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
        //逐行读取，按行处理
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            //切割一行
            //A : A 的关注数
            String[] split = line.split("\t");
            followeesCountMap.put(Long.parseLong(split[0]), Integer.parseInt(split[1]));
        }
        //关流
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        // 获取一行
        String line = value.toString();
        String[] words = line.split("\t");
        String[] keys = words[0].split(",");
        outKey.set(keys[0] + "-" + followeesCountMap.get(Long.parseLong(keys[0])));
        outValue.set(keys[1] + "-" + followeesCountMap.get(Long.parseLong(keys[1])) + "|" + words[1]);
        context.write(outKey, outValue);
        outKey.set(keys[1] + "-"  + followeesCountMap.get(Long.parseLong(keys[1])));
        outValue.set(keys[0] + "-" + followeesCountMap.get(Long.parseLong(keys[0])) + '|' + words[1]);
        context.write(outKey, outValue);
    }
}
