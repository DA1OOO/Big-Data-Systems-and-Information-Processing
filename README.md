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
