package com.dai.mapreduce.communitydetection.mapreduce3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

/**

 */
public class BlogDetection3Reducer extends Reducer<Text, Text, LongWritable, Text> {
    private LongWritable outKey = new LongWritable();

    private Text outValue = new Text();

    // 用优先队列构造容量为3的最小堆
    private PriorityQueue<ValueAndSimilarity> minheap = new PriorityQueue<>(3, new Comparator<ValueAndSimilarity>() {
        // 重写comparator
        @Override
        public int compare(ValueAndSimilarity o1, ValueAndSimilarity o2) {
            return o1.getSimilarity() - o2.getSimilarity() > 0 ?  1 : -1;
        }
    });

    class ValueAndSimilarity {
        Text value;
        Double similarity;

        public void set(Text value, Double similarity) {
            this.value = value;
            this.similarity = similarity;
        }

        public Text getValue() {
            return this.value;
        }

        public Double getSimilarity() {
            return similarity;
        }

        // 无参构造方法
        public ValueAndSimilarity() {
        }

        //有参构造方法
        public ValueAndSimilarity(Text value, Double similarity) {
            this.value = value;
            this.similarity = similarity;
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
        // A-4 分割为 [A , 4]
        String[] tempInfo = key.toString().split("-");
        if (!tempInfo[0].endsWith("2964")) return;
        outKey.set(Long.parseLong(tempInfo[0]));
        // 记录key关注者的个数
        int followeesNum = Integer.parseInt(tempInfo[1]);
        int otherFolloweesNum = 0;
        double similarity = 0;
        ValueAndSimilarity valueAndSimilarity = new ValueAndSimilarity();
        for (Text value : values) {
            // D-5|[B,C,E] 分割为 [ D-5, [B,C,E] ]
            String[] temp = value.toString().split("\\|");
            String[] commonFollowees = temp[1].split(", ");
            // D-5 分割为 [D, 5]
            String[] temp2= temp[0].split("-");
            String tempStr = temp2[0] + "," + temp[1];
            otherFolloweesNum = Integer.parseInt(temp2[1]);
            // 计算相似度的公式
            similarity = (double)commonFollowees.length / ((double)followeesNum + (double)otherFolloweesNum - (double)commonFollowees.length);
            if (minheap.size() < 3) {
                minheap.add(new ValueAndSimilarity(new Text(tempStr), similarity));
            } else {
                if (similarity > minheap.peek().similarity) {
                    minheap.add(new ValueAndSimilarity(new Text(tempStr), similarity));
                    minheap.poll();
                }
            }
        }
        for (ValueAndSimilarity elem : minheap) {
            outValue.set(tempInfo[0] + ":" + elem.getValue().toString() + "," + elem.getSimilarity());
            context.write(outKey, outValue);
        }
        minheap.clear();
    }
}
