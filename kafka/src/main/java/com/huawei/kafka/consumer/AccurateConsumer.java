package com.huawei.kafka.consumer;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;

/**
 * @author: xuqiangnj@163.com
 * @date: 2019/5/3 14:36
 * @description:精确一次消费实现
 */
public class AccurateConsumer {

    private static final Properties props = new Properties();

    private static final String GROUP_ID = "Test";

    static {
        props.put("bootstrap.servers", "192.168.142.139:9092");
        props.put("group.id", GROUP_ID);
        //注意这里设置为手动提交方式
        props.put("enable.auto.commit", false);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    final KafkaConsumer<String, String> consumer;

    //用于记录每次消费时每个partition的最新offset
    private Map<TopicPartition, Long> partitionOffsetMap;

    //用于缓存接受消息,然后进行批量入库
    private List<Message> list;

    private volatile boolean isRunning = true;

    private final String topicName;

    private final String topicNameAndGroupId;

    public AccurateConsumer(String topicName) {
        this.topicName = topicName;
        topicNameAndGroupId = topicName + "_" + GROUP_ID;
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName), new HandleRebalance());
        list = new ArrayList<>(100);
        partitionOffsetMap = new HashMap<>();
    }

    //这里使用异步提交和同步提交的组合方式
    public void receiveMsg() {
        try {
            while (isRunning) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                if (!consumerRecords.isEmpty()) {
                    for (TopicPartition topicPartition : consumerRecords.partitions()) {
                        List<ConsumerRecord<String, String>> records = consumerRecords.records(topicPartition);
                        for (ConsumerRecord<String, String> record : records) {
                            //使用fastjson将记录中的值转换为Message对象,并添加到list中
                            list.addAll(JSON.parseArray(record.value(), Message.class));
                        }
                        //将partition对应的offset信息添加到map中,入库时将offset-partition信息一起进行入库
                        partitionOffsetMap.put(topicPartition, records.get(records.size() - 1)
                                .offset() + 1);//记住这里一定要加1,因为下次消费的位置就是从+1的位置开始
                    }
                }
                //如果list中存在有数据,则进行入库操作
                if (list.size() > 0) {
                    boolean isSuccess = insertIntoDB(list, partitionOffsetMap);
                    if (isSuccess) {
                        //将缓存数据清空,并将offset信息清空
                        list.clear();
                        partitionOffsetMap.clear();
                    }
                }
            }
        } catch (Exception e) {
            //处理异常
        } finally {
            //offset信息由我们自己保存,提交offset其实没有什么必要
            //consumer.commitSync();
            close();
        }

    }

    private boolean insertIntoDB(List<Message> list, Map<TopicPartition, Long> partitionOffsetMap) {
        Connection connection = getConnection();//获取数据库连接 自行实现
        boolean flag = false;
        try {
            //设置手动提交,让插入数据和更新offset信息在一个事务中完成
            connection.setAutoCommit(false);

            //将数据进行入库 自行实现
            insertMessage(list);

            //更新offset信息 自行实现
            updateOffset(partitionOffsetMap);

            connection.commit();
            flag = true;
        } catch (SQLException e) {
            try {
                //出现异常则回滚事务
                connection.rollback();
            } catch (SQLException e1) {
                //处理异常
            }
        }
        return flag;
    }

    private void updateOffset(Map<TopicPartition, Long> partitionOffsetMap) {
    }

    private void insertMessage(List<Message> list) {
    }

    //获取数据库连接 自行实现
    private Connection getConnection() {
        return null;
    }

    public void close() {
        isRunning = false;
        if (consumer != null) {
            consumer.close();
        }
    }

    private class HandleRebalance implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            //发生Rebalance时,只需要将list中数据和记录offset信息清空即可
            //这里为什么要清除数据,应为在Rebalance的时候有可能还有一批缓存数据在内存中没有进行入库，
            //并且offset信息也没有更新,如果不清除,那么下一次还会重新poll一次这些数据,将会导致数据重复
            list.clear();
            partitionOffsetMap.clear();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            //获取对应Topic的分区数
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topicName);
            Map<TopicPartition, Long> partitionOffsetMapFromDB = getPartitionOffsetMapFromDB
                    (partitionInfos.size());

            //在分配分区时指定消费位置
            for (TopicPartition partition : partitions) {
                //如果在数据库中有对应partition的信息则使用，否则将默认从offset=0开始消费
                if (partitionOffsetMapFromDB.get(partition) != null) {
                    consumer.seek(partition, partitionOffsetMapFromDB.get(partition));
                } else {
                    consumer.seek(partition, 0L);
                }
            }
        }
    }

    /**
     * 从数据库中查询分区和offset信息
     *
     * @param size 分区数量
     * @return 分区号和offset信息
     */
    private Map<TopicPartition, Long> getPartitionOffsetMapFromDB(int size) {
        Map<TopicPartition, Long> partitionOffsetMapFromDB = new HashMap<>();
        //从数据库中查询出对应信息
        Connection connection = getConnection();//获取数据库连接 自行实现
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        String querySql = "SELECT partition_num,offsets from kafka_info WHERE topic_group = ?";
        try {
            preparedStatement = connection.prepareStatement(querySql);
            preparedStatement.setString(1, topicNameAndGroupId);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                partitionOffsetMapFromDB.put(new TopicPartition(topicName, resultSet.getInt(1)),
                        resultSet.getLong(2));
            }
            //判断数据库是否存在所有的分区的信息,如果没有,则需要进行初始化
            if (partitionOffsetMapFromDB.size() < size) {
                connection.setAutoCommit(false);
                StringBuilder sqlBuilder = new StringBuilder();
                //partition分区号是从0开始,如果有10个分区,那么分区号就是0-9
                /*这里拼接插入数据 格式 INSERT INTO kafka_info(topic_group_partition,topic_group,partition_num) VALUES
                (topicNameAndGroupId_0,topicNameAndGroupId,0),(topicNameAndGroupId_1, topicNameAndGroupId,1)....*/
                for (int i = 0; i < size; i++) {
                    sqlBuilder.append("(").append
                            (topicNameAndGroupId).append("_").append(i).append(",").append
                            (topicNameAndGroupId).append(",").append(i).append("),");
                }
                //将最后一个逗号去掉加上分号结束
                sqlBuilder.deleteCharAt(sqlBuilder.length() - 1).append(";");
                preparedStatement = connection.prepareStatement("INSERT INTO kafa_info" +
                        "(topic_group_partition,topic_group,partition_num) VALUES " + sqlBuilder.toString());
                preparedStatement.execute();
                connection.commit();
            }
        } catch (SQLException e) {
            //处理异常 回滚事务 这里应该结束程序 排查错误
            try {
                connection.rollback();
            } catch (SQLException e1) {
                //打印日志 排查错误信息
            }

        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                //处理异常 打印日志即可 关闭资源失败
            }
        }
        return partitionOffsetMapFromDB;
    }
}
