import os
import random
import time

# modify this function to convert the ts to seconds  
def convert_to_seconds(ts): 
    return ts

# modify this function, the sleep time should based on the time in the data
def random_sleep(ts):

    t = random.randint(1, 4) # modify this line
    time.sleep(t)

def main():
    last_ts = None
    with open('bitcoin_twitter.txt') as f:
        for line in f:

            # split the text and timestamp
            parts = line.rstrip().split(',')
            text = ' '.join(parts[:-1])
            ts = parts[-1]

            ts = convert_to_seconds(ts)     

            cmd = 'echo "' + text + '" | ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic bitcoin'
            
            os.system(cmd)

            if last_ts is not None:
                random_sleep(ts)

            last_ts = ts


if __name__ == '__main__':
    main()