Intro
============

Kangaroo is Conductor's scalable Hadoop Map/Reduce `InputFormat` for Kafka.

# Setting up Kangaroo

You can build Kangaroo with:

```mvn clean package```

# Using Kangaroo

## Create a Mapper
```java
public static class MyMapper extends Mapper<LongWritable, BytesWritable, KEY_OUT, VALUE_OUT> {
    // implementation
}
```

## Single topic

```java
// Create a new job
final Job job = Job.getInstance(getConf(), "my_job");

// Set the InputFormat
job.setInputFormatClass(KafkaInputFormat.class);

// Set your Zookeeper connection string
KafkaInputFormat.setZkConnect(job, "zookeeper-1.xyz.com:2181");

// Set the topic you want to consume
KafkaInputFormat.setTopic(job, "my_topic");

// Set the consumer group associated with this job
KafkaInputFormat.setConsumerGroup(job, "my_consumer_group");

// Set the mapper that will consume the data
job.setMapperClass(MyMapper.class);

// (Optional) Only commit offsets if the job is successful
if (job.waitForCompletion(true)) {
    final ZkUtils zk = new ZkUtils(job.getConfiguration());
    zk.commit("my_consumer_group", "my_topic");
    zk.close();
}
```

## Multiple topics
```java
// Create a new job
final Job job = Job.getInstance(getConf(), "my_job");

// Set the InputFormat
job.setInputFormatClass(MultipleKafkaInputFormat.class);

// Set your Zookeeper connection string
KafkaInputFormat.setZkConnect(job, "zookeeper-1.xyz.com:2181");

// Add as many queue inputs as you'd like
MultipleKafkaInputFormat.addTopic(job, "my_first_topic", "my_consumer_group", MyMapper.class);
MultipleKafkaInputFormat.addTopic(job, "my_second_topic", "my_consumer_group", MyMapper.class);
// ...

// (Optional) Only commit offsets if the job is successful
if (job.waitForCompletion(true)) {
    final ZkUtils zk = new ZkUtils(job.getConfiguration());
    // commit the offsets for each topic
    zk.commit("my_consumer_group", "my_first_topic");
    zk.commit("my_consumer_group", "my_second_topic");
    // ...
    zk.close();
}
```