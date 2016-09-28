package com.ligx.KafkaMonitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ligx.util.ColorUtil;

public class KafkaStateMonitor implements Watcher {

  private CountDownLatch latch = new CountDownLatch(1);
  private ZooKeeper zk = null;

  public KafkaStateMonitor() {
    init();
  }

  private void init() {
    try {
//      zk = new ZooKeeper("ukafka-u5qna5-1-bj03.service.ucloud.cn:2181", 500000, this);
      zk = new ZooKeeper("10.10.221.163:2181", 500000, this);

      if (zk.getState() == States.CONNECTING) {
        try {
          latch.await();
        } catch (InterruptedException e) {
          throw new IllegalStateException(e);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void close() {
    try {
      zk.close();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void process(WatchedEvent event) {
    if (event.getState() == KeeperState.SyncConnected) {
      latch.countDown();
    }
  }

  public void parseJson(String jsonStr, int type) {
    JSONObject jo = JSON.parseObject(jsonStr);
    Map<String, String> result = new HashMap<String, String>();

    switch (type) {
      case KafkaNode.BROKER:
        result.put("jmx端口号", String.valueOf(jo.getInteger("jmx_port")));
        result.put("时间戳", jo.getString("timestamp"));
        result.put("host", jo.getString("host"));
        result.put("版本号", String.valueOf(jo.getInteger("version")));
        result.put("broker端口号", String.valueOf(jo.getIntValue("port")));
        break;
      case KafkaNode.TOPIC:

    }

  }

  /**
   * 获取Broker注册信息
   */
  public Map<String, String> getBrokerInfo() throws KeeperException, InterruptedException {
    Map<String, String> brokerInfo = new HashMap<String, String>();
    List<String> brokerIds = zk.getChildren("/brokers/ids", false);
    for (String brokerId : brokerIds) {
      byte[] bytes = zk.getData("/brokers/ids/" + brokerId, false, null);
      if (bytes != null && bytes.length > 0) {
        brokerInfo.put(brokerId, new String(bytes));
      }
    }
    return brokerInfo;
  }

  public String getBrokerInfoForApi() throws KeeperException, InterruptedException {
    return JSON.toJSONString(getBrokerInfo(), true);
  }

  /**
   * 获取Topic注册信息
   */
  public Map<String, String> getTopicInfo() throws KeeperException, InterruptedException {
    Map<String, String> topicInfo = new HashMap<String, String>();
    List<String> topics = zk.getChildren("/brokers/topics", false);
    for (String topic : topics) {
      byte[] bytes = zk.getData("/brokers/topics/" + topic, false, null);
      if (bytes != null && bytes.length > 0) {
        topicInfo.put(topic, new String(bytes));
      }
    }
    return topicInfo;
  }

  public String getTopicInfoForApi() throws KeeperException, InterruptedException {
    return JSON.toJSONString(getTopicInfo(), true);
  }

  /**
   * 获取每个Topic下所有Partition状态信息
   */
  public Map<String, Map<String, String>> getTopicPartitionInfo()
      throws KeeperException, InterruptedException {
    Map<String, Map<String, String>> topicPartitionInfo = new HashMap<String, Map<String, String>>();
    // 所有的topic
    Map<String, String> topicInfo = getTopicInfo();

    if (topicInfo != null) {
      Set<String> topicSet = topicInfo.keySet();
      for (String topic : topicSet) {
        List<String> partitionIdList = zk
            .getChildren("/brokers/topics/" + topic + "/partitions", false);
        Map<String, String> partitionInfo = new HashMap<String, String>();
        if (partitionIdList != null && !partitionIdList.isEmpty()) {
          for (String partitionId : partitionIdList) {
            byte[] bytes = zk
                .getData("/brokers/topics/" + topic + "/partitions/" + partitionId + "/state",
                    false, null);
            if (bytes != null && bytes.length > 0) {
              partitionInfo.put(partitionId, new String(bytes));
            }
          }
        }
        topicPartitionInfo.put(topic, partitionInfo);
      }
    }
    topicPartitionInfo.remove("__consumer_offsets");
    return topicPartitionInfo;
  }

  public String getTopicPartitionInfoForApi() throws KeeperException, InterruptedException {
    return JSON.toJSONString(getTopicPartitionInfo(), true);
  }

  /**
   * 获取每个ConsumerGroup中的Consumer注册信息
   */
  public Map<String, Map<String, Map<String, String>>> getGroupConsumerInfo()
      throws KeeperException, InterruptedException {
    Map<String, Map<String, Map<String, String>>> cgConsumersInfo = new HashMap<>();

    List<String> groupIds = zk.getChildren("/consumers", false);
    for (String groupId : groupIds) {
      Stat stat = zk.exists("/consumers/" + groupId + "/ids", false);
      if (stat != null) {
        // 当前Consumer Group下，所有的consumer信息
        Map<String, Map<String, String>> consumerInfo = new HashMap<>();
        List<String> consumerIds = zk.getChildren("/consumers/" + groupId + "/ids", false);
        for (String consumerId : consumerIds) {
          byte[] bytes = zk.getData("/consumers/" + groupId + "/ids/" + consumerId, false, null);
          if (bytes.length > 0) {
            consumerInfo.put(consumerId, JSON.parseObject(new String(bytes), Map.class));
          }
        }
        cgConsumersInfo.put(groupId, consumerInfo);
      }
    }
    return cgConsumersInfo;
  }

  public String getGroupConsumerInfoForApi() throws KeeperException, InterruptedException {
    return JSON.toJSONString(getGroupConsumerInfo(), true);
  }

  /**
   * /consumers/[groupId]/owners/[topic]/[partitionId] -> consumerIdString + threadId索引编号
   * @throws KeeperException
   * @throws InterruptedException
   */
  private List<String> getTopicPartitionOwner() throws KeeperException, InterruptedException {
    // 所有的ConsumerGroup
    List<String> groupIds = zk.getChildren("/consumers", false);
    // 所有的topic
    Set<String> topics = getTopicInfo().keySet();

    List<String> result = new ArrayList<>();

    for(String groupId : groupIds){
      for(String topic : topics){
        if(zk.exists("/consumers/" + groupId + "/owners/" + topic, false) == null){
          continue;
        }
        List<String> partitions = zk.getChildren("/consumers/" + groupId + "/owners/" + topic, false);
        if(partitions != null){
          for(String partition : partitions){
            byte[] bytes = zk.getData("/consumers/" + groupId + "/owners/" + topic + "/" + partition, false, null);
            if(bytes != null){
              String consumer = new String(bytes);
              result.add(String.format("%s - %s - %s - %s", groupId, topic, partition, consumer));
            }
          }
        }
      }
    }
    return result;
  }

  public String getTopicPartitionOwnerForApi(){
    try {
      List<String> result = getTopicPartitionOwner();
      return JSON.toJSONString(result, true);
    } catch (KeeperException | InterruptedException e) {
      e.printStackTrace();
    }
    return "";
  }

  /**
   * 获取每个Topic下所有Partition中被某个ConsumerGroup中的消费者消费的最大偏移量offset
   */
  private List<GroupTopicMessageOffset> getPartitionOffsetInfo()
      throws KeeperException, InterruptedException {
    List<GroupTopicMessageOffset> topicPartitionMessageOffsetInfo = new ArrayList<GroupTopicMessageOffset>();
    // 所有的ConsumerGroup
    List<String> groupIds = zk.getChildren("/consumers", false);
    // 所有的topic
    Set<String> topics = getTopicInfo().keySet();

    for (String groupId : groupIds) {
      for (String topic : topics) {
        Stat stat = zk.exists("/consumers/" + groupId + "/offsets/" + topic, false);
        if (stat != null) {
          List<String> partitionIds = zk
              .getChildren("/consumers/" + groupId + "/offsets/" + topic, false);
          for (String partitionId : partitionIds) {
            byte[] bytes = zk
                .getData("/consumers/" + groupId + "/offsets/" + topic + "/" + partitionId, false,
                    null);
            if (bytes.length > 0) {
              GroupTopicMessageOffset gtmo = new GroupTopicMessageOffset(groupId, topic,
                  partitionId, new String(bytes));
              topicPartitionMessageOffsetInfo.add(gtmo);
            }
          }
        }
      }
    }
    return topicPartitionMessageOffsetInfo
        .size() > 0 ? topicPartitionMessageOffsetInfo : new ArrayList<>();
  }

  /**
   * 利用Consumer均衡算法，计算得到每个Consumer与之对应的Partition
   */
  private List<Consumer2Partition> getConsumerPartition()
      throws KeeperException, InterruptedException {
    List<Consumer2Partition> c2pL = new ArrayList<Consumer2Partition>();

    Map<String, Map<String, Map<String, String>>> groupConsumerInfo = getGroupConsumerInfo();
    Map<String, Map<String, String>> topicPartitionInfo = getTopicPartitionInfo();

    for (Map.Entry<String, Map<String, Map<String, String>>> cgConsumerME : groupConsumerInfo.entrySet()) {
      // 指定Consumer Group下，Consumer的数量
      String groupId = cgConsumerME.getKey();
      double consumerCount = cgConsumerME.getValue().size();

      for (Map.Entry<String, Map<String, String>> me : topicPartitionInfo.entrySet()) {
        // 指定Topic下Partition数量
        String topic = me.getKey();
        double partitionCount = me.getValue().size();

        //Partition数量与Consumer数比值(向上取整)
        int m = (int) Math.ceil(partitionCount / consumerCount);

        for (int consumerIndex = 0; consumerIndex < consumerCount; consumerIndex++) {
          int startP = consumerIndex * m;
          int endP = (consumerIndex + 1) * m - 1;
          //groupId消费组中第i个Consumer消费topic中的startP到endP
          Consumer2Partition c2p = new Consumer2Partition(groupId, consumerIndex, topic, startP,
              endP);
          c2pL.add(c2p);
        }
      }
    }
    return c2pL.size() > 0 ? c2pL : new ArrayList<>();
  }

  /**
   * 利用上面得到的数据，计算每个consumer的消费信息
   */
  public List<String> getConsumerOffset() throws KeeperException, InterruptedException {
    // 获取每个Topic下所有Partition中被某个ConsumerGroup中的消费者消费的最大偏移量offset
    List<GroupTopicMessageOffset> topicPartitionMessageOffsetInfo = getPartitionOffsetInfo();
    // 每个Consumer与之对应的Partition
    List<Consumer2Partition> c2pL = getConsumerPartition();
    List<String> result = new ArrayList<>();

    for (GroupTopicMessageOffset gtmo : topicPartitionMessageOffsetInfo) {
      boolean hasConsumerForCGAndTopic = false;

      for (Consumer2Partition c2p : c2pL) {
        // 找到消费指定Topic并且在指定CG中的Consumer
        if (c2p.getG().equals(gtmo.getGroup()) && c2p.getTopic().equals(gtmo.getTopic())) {
          hasConsumerForCGAndTopic = true;
          int lookupP = Integer.parseInt(gtmo.getPartition());
          int startP = c2p.getStartP();
          int endP = c2p.getEndP();
          if ((lookupP > startP && lookupP < endP) || lookupP == startP || lookupP == endP) {
            result.add(ColorUtil.white("消费组【") + ColorUtil.green(c2p.getG()) +
                ColorUtil.white("】的【") + ColorUtil.red("消费者 " + String.valueOf(c2p.getC())) +
                ColorUtil.white("】在topic【") + ColorUtil.blue(c2p.getTopic()) +
                ColorUtil.white("】的【") + ColorUtil.yellow("partition " + String.valueOf(lookupP)) +
                ColorUtil.white("】上消费的偏移量为") + ColorUtil.yellow(gtmo.getMessageOffset()));
          }
        }
      }

      if (!hasConsumerForCGAndTopic) {
        result.add(ColorUtil.white("消费组【") + ColorUtil.green(gtmo.getGroup()) +
            ColorUtil.white("】的【") + ColorUtil.red("None") +
            ColorUtil.white("】在topic【") + ColorUtil.blue(gtmo.getTopic()) +
            ColorUtil.white("】的【") + ColorUtil.yellow("partition " + gtmo.getPartition()) +
            ColorUtil.white("】上消费的偏移量为") + ColorUtil.yellow(gtmo.getMessageOffset()));
      }
    }
    return result;
  }

  public String getConsumerOffsetInfoForApi() throws KeeperException, InterruptedException {
    return JSON.toJSONString(getConsumerOffset(), true);
  }


  public static void main(String[] args) throws KeeperException, InterruptedException {
    KafkaStateMonitor monitor = new KafkaStateMonitor();

    System.out.println("---------------broker信息---------------");

    Map<String, String> brokerInfo = monitor.getBrokerInfo();
    for (Map.Entry<String, String> me : brokerInfo.entrySet()) {
      System.out.println("broker " + me.getKey() + ":" + me.getValue());
    }

    System.out.println("--------------topic信息----------------");

    Map<String, String> topicInfo = monitor.getTopicInfo();
    for (Map.Entry<String, String> me : topicInfo.entrySet()) {
      System.out.println("topic " + me.getKey() + ":" + me.getValue());
    }

    System.out.println("---------------partition信息---------------");

    Map<String, Map<String, String>> topicPartitionInfo = monitor.getTopicPartitionInfo();
    for (Map.Entry<String, Map<String, String>> me : topicPartitionInfo.entrySet()) {
      System.out.println(me.getKey());
      for (Map.Entry<String, String> m : me.getValue().entrySet()) {
        System.out.println(m.getKey() + ":" + m.getValue());
      }
    }

    System.out.println("---------------consumer信息---------------");

    Map<String, Map<String, Map<String, String>>> cgConsumersInfo = monitor.getGroupConsumerInfo();
    for (Map.Entry<String, Map<String, Map<String, String>>> me : cgConsumersInfo.entrySet()) {
      System.out.println("消费组: " + me.getKey());
      for (Map.Entry<String, Map<String, String>> m : me.getValue().entrySet()) {
        System.out.println(m.getKey() + ":" + JSON.toJSONString(m.getValue()));
      }
    }


    System.out.println("----------------consumer与offset信息--------------");

    List<String> consumerOffsetList = monitor.getConsumerOffset();
    consumerOffsetList.forEach(System.out::println);


    monitor.close();
  }

  public interface KafkaNode {
    public final int BROKER = 0;

    public final int TOPIC = 1;

    public final int PARTITION = 2;

    public final int CONSUMER = 3;
  }
}
