name := "Akka-in-action"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.4.9",
  "com.typesafe.akka" %% "akka-remote" % "2.4.9",
  "com.typesafe.akka" %% "akka-http-experimental" % "2.4.9",
  "com.typesafe.akka" %% "akka-http-spray-json-experimental" % "2.4.9",
  "org.apache.kafka" %% "kafka" % "0.9.0.1",
  "com.typesafe.slick" %% "slick" % "3.1.1",
  "org.scalatest" %% "scalatest" % "3.0.0-M15",
  "org.apache.thrift" % "libthrift" % "0.8.0",
  "com.alibaba" % "fastjson" % "1.2.12",
  "org.fusesource.jansi" % "jansi" % "1.13")