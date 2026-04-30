### 介绍

基于 [flink-obs](https://gitcode.com/HuaweiCloudDeveloper/flink-obs) 项目，更新 flink 版本为: 1.20.3

### Build
```
export GPG_TTY=$(tty)
mvn clean deploy -Pdeploy
```

```xml
<dependency>
    <groupId>com.gitee.melin.huaweicloud</groupId>
    <artifactId>flink-obs-fs-hadoop</artifactId>
    <version>1.0.0_1.20</version>
</dependency>