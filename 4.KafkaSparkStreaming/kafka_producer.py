# coding=utf-8
import os
import time


# 取出时间中的秒数
def convert_to_seconds(ts):
    second = ts[-2:]
    return second


def main():
    last_ts = None
    with open(
            "C:\Users\Administrator\Desktop\Big-Data-Systems-and-Information-Processing\4.KafkaSparkStreaming\data\new_tweets.txt") as f:
        for line in f:
            # 划分文本和时间戳
            parts = line.rstrip().split(',')
            text = ' '.join(parts[:-1])
            ts = parts[-1]
            # 当前推文时间戳
            ts = convert_to_seconds(ts)
            # 向Kafka生产者发送消息
            cmd = 'echo "' + text + '" | kafka-console-producer.sh --broker-list hadoop3:9092 --topic bitcoin'
            os.system(cmd)
            # 模拟时间间隔
            if last_ts is not None:
                time.sleep(abs(int(ts) - int(last_ts)) % 5)
            last_ts = ts
            print("===> a message have be sent to kafka topic: " + text)


if __name__ == '__main__':
    main()
