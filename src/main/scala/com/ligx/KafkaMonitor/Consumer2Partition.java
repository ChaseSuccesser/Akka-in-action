package com.ligx.KafkaMonitor;

public class Consumer2Partition {

  private String G;
  private int C;
  private String topic;
  private int startP;
  private int endP;

  public Consumer2Partition(String G, int c, String topic, int startP, int endP) {
    this.G = G;
    this.C = c;
    this.topic = topic;
    this.startP = startP;
    this.endP = endP;
  }

  public String getG() {
    return G;
  }

  public void setG(String g) {
    G = g;
  }

  public int getC() {
    return C;
  }

  public void setC(int c) {
    C = c;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public int getStartP() {
    return startP;
  }

  public void setStartP(int startP) {
    this.startP = startP;
  }

  public int getEndP() {
    return endP;
  }

  public void setEndP(int endP) {
    this.endP = endP;
  }

}
