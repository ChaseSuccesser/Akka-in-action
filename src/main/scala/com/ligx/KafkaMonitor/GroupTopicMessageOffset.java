package com.ligx.KafkaMonitor;

public class GroupTopicMessageOffset {

  private String group;
  private String topic;
  private String partition;
  private String messageOffset;

  public GroupTopicMessageOffset(String group, String topic, String partition,
      String messageOffset) {
    this.group = group;
    this.topic = topic;
    this.partition = partition;
    this.messageOffset = messageOffset;
  }

  public String getGroup() {
    return group;
  }

  public void setGroup(String group) {
    this.group = group;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getPartition() {
    return partition;
  }

  public void setPartition(String partition) {
    this.partition = partition;
  }

  public String getMessageOffset() {
    return messageOffset;
  }

  public void setMessageOffset(String messageOffset) {
    this.messageOffset = messageOffset;
  }
}
