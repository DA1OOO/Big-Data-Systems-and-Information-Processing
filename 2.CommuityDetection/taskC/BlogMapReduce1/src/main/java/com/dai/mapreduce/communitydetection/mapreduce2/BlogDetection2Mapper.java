package com.dai.mapreduce.communitydetection.mapreduce2;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class BlogDetection2Mapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    // 该map输出的key
    private IntWritable outKey = new IntWritable();
    // 该map输出的value
    private IntWritable outValue = new IntWritable();

    private Map<Long, Integer> isCommonFollowee = new HashMap<>();

    @Override
    protected void setup(Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
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
            //A : A是否是common followee
            String[] split = line.split("\t");
            isCommonFollowee.put(Long.parseLong(split[0]), Integer.parseInt(split[1]));
        }
        //关流
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        String blog = words[0];
        String label = words[1];
        outKey.set(Integer.parseInt(label));
        if (isCommonFollowee.get(Long.parseLong(blog)) != null) {
            outValue.set(isCommonFollowee.get(Long.parseLong(blog)));
        } else {
            outValue.set(0);
        }
        context.write(outKey, outValue);
    }
}
