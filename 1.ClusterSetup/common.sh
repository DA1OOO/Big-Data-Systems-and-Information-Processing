tar -zxvf jdk-8u351-linux-x64.tar.gz -C opt/module/
sudo vim /etc/profile.d/my_env.sh
source /etc/profile
java -version
tar -zxvf hadoop-2.9.2.tar.gz -C opt/module/
sudo vim /etc/profile.d/my_env.sh
hdfs namenode -format
start-dfs.sh
vim hadoop-env.sh
hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.2.jar teragen 100000 terasort/input
hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.2.jar terasort terasort/input terasort/output
hadoop jar ./share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.2.jar teravalidate terasort/output terasort/check
sudo vim /etc/hostname
sudo vim /etc/hosts
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
rsync -av /home/dai_hk/opt/module/hadoop-2.9.2/etc/hadoop/ dai_hk@hadoop4:/home/dai_hk/opt/module/hadoop-2.9.2/etc/hadoop/
xxxxxxxxxx start-hdfs.shstart-yarn.sh