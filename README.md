# Big-Data-Systems-and-Information-Processing

## 1. Hadoop Cluster Setup

<img src="./README.assets/截屏2023-01-12 16.48.58.png" alt="截屏2023-01-12 16.48.58" style="zoom:50%;" />

### Create Virtual Machine

Hadoop-1 Settings:

1. Ubuntu 18.04 LTS
2. 100GB Hard disk
3. 8G RAM
4. 2 CPU core

### Firewall Settings

Create a new rule to the inbound/ ingress. Only allow CUHK IP access VMs.

<img src="./README.assets/截屏2023-01-12 17.34.00.png" alt="截屏2023-01-12 17.34.00" style="zoom:80%;" />

### JDK Install

1. Download jdk8 from Oracle, and import it into hadoop-1 virtual machine.

![image-20230112184127457](./README.assets/image-20230112184127457.png)

2. Extract it to `/opt/software`.

   ```shell
   tar -zxvf jdk-8u351-linux-x64.tar.gz -C opt/module/
   ```

   Then we get `jdk1.8.0_351` in `/opt/module`

   ![image-20230112184628352](./README.assets/image-20230112184628352.png)

   

3. Configure environment variables in `my_env.sh`.

   ```shell
   sudo vim /etc/profile.d/my_env.sh
   ```

   `my_env.sh` :

   ```shell
   #JAVA_HOME
   export JAVA_HOME=/home/dai_hk/opt/module/jdk1.8.0_351
   export PATH=$PATH:$JAVA_HOME/bin
   ```

4. Then, make the new environment variable effective.

   ```shell
   source /etc/profile
   ```

5. Check whether the JDK8 is installed successfully.

   ```shell
   java -version
   ```

   The following figure shows that the JDK installation was successful.

   ![image-20230113112200916](README.assets/image-20230113112200916.png)

### Hadoop Install

1. Download `hadoop-2.9.2.tar.gz`

![image-20230113165403754](README.assets/image-20230113165403754.png)

2. Extract it to `/opt/software`.

   ```shell
   tar -zxvf hadoop-2.9.2.tar.gz -C opt/module/
   ```

   Then we get `hadoop-2.9.2` in `/opt/module`

   ![image-20230113165524841](README.assets/image-20230113165524841.png)

3. Configure environment variables in `my_env.sh`.

```shell
sudo vim /etc/profile.d/my_env.sh
```

​		`my_env.sh` :

```shell
#HADOOP_HOME
export HADOOP_HOME=/home/dai_hk/opt/module/hadoop-2.9.2
export PATH=$PATH:$HADOOP_HOME/bin
export PATH=$PATH:$HADOOP_HOME/sbin
```

4. Then, make the new environment variable effective.

```shell
source /etc/profile
```

5. Check whether the Hadoop is installed successfully.

```shell
hadoop
```

The following figure shows that the Hadoop installation was successful.

<img src="README.assets/image-20230113170224422.png" alt="image-20230113170224422" style="zoom:50%;" />

### Single-node Hadoop Setup

### Wordcount

1. Create a file folder called `/wcinpt`.

   ```shell 
   mkdir /wcinput
   ```

2. Creat a txt file `word.txt`, input some random strings.

   ```shell
   vim word.txt
   ```

   Enter some strings:

   ![image-20230113173818624](README.assets/image-20230113173818624.png)

3. Enter Hadoop-2.9.2 file, then open `hadoop-mapreduce-examples-2.9.2`, execute **wordcount** commands, using `/wcinput` as input, and output to `/wcoutput`.

   ```shell
   hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.2.jar wordcount wcinput/ wcoutput
   ```

![image-20230113172656667](README.assets/image-20230113172656667.png)

4. Watch the result of **wordcount** program.

   ![image-20230113173530630](README.assets/image-20230113173530630.png)

   ```shell
   vim part-r-0000
   ```

   **wordcount** result as following picture show:

​					![image-20230113173557389](README.assets/image-20230113173557389.png)
