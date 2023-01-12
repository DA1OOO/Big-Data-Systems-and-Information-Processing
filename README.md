# Big-Data-Systems-and-Information-Processing

## 1. Hadoop Cluster Setup

<img src="/Users/daiyayun/Desktop/Big-Data-Systems-and-Information-Processing/README.assets/截屏2023-01-12 16.48.58.png" alt="截屏2023-01-12 16.48.58" style="zoom:80%;" />

### Create Virtual Machine

Hadoop-1 Settings:

1. Ubuntu 18.04 LTS
2. 100GB Hard disk
3. 8G RAM
4. 2 CPU core

### Firewall Settings

Create a new rule to the inbound/ ingress. Only allow CUHK IP access VMs.

![截屏2023-01-12 17.34.00](/Users/daiyayun/Desktop/Big-Data-Systems-and-Information-Processing/README.assets/截屏2023-01-12 17.34.00.png)

### JDK Install

1. Download jdk8 from Oracle, and import it into hadoop-1 virtual machine

![image-20230112184127457](/Users/daiyayun/Desktop/Big-Data-Systems-and-Information-Processing/README.assets/image-20230112184127457.png)

2. Extract it to `/opt/software`

   ```shell
   tar -zxvf jdk-8u351-linux-x64.tar.gz -C opt/module/
   ```

   Then we get `jdk1.8.0_351` in `/opt/module`

   ![image-20230112184628352](/Users/daiyayun/Desktop/Big-Data-Systems-and-Information-Processing/README.assets/image-20230112184628352.png)

   

3. 