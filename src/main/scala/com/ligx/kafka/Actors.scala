package com.ligx.kafka

import scala.concurrent.duration._
import akka.actor.{Actor, ActorRef, Cancellable, FSM, Props}
import com.ligx.kafka.ConnectorFSM._
import com.ligx.kafka.StreamFSM._
import kafka.consumer.{ConsumerConnector, ConsumerTimeoutException, KafkaStream, TopicFilter}
import kafka.message.MessageAndMetadata

/**
  * Created by ligx on 16/6/20.
  */
object ConnectorFSM{

  sealed trait ConnectorState

  case object Committing extends ConnectorState

  case object Receiving extends ConnectorState

  case object Stopped extends ConnectorState

  sealed trait ConnectorProtocol

  case object Start extends ConnectorProtocol

  case class Drained(stream: String) extends ConnectorProtocol

  case object Received extends ConnectorProtocol

  case object Commit extends ConnectorProtocol

  case object Started extends ConnectorProtocol

  case object Committed extends ConnectorProtocol

  case object Stop extends ConnectorProtocol
}


object StreamFSM {

  sealed trait StreamState

  case object Processing extends StreamState

  case object Full extends StreamState

  case object Draining extends StreamState

  case object Empty extends StreamState

  case object Unused extends StreamState

  case object FlattenContinue extends StreamState

  sealed trait StreamProtocol

  case object StartProcessing extends StreamProtocol

  case object Drain extends StreamProtocol

  case object Processed extends StreamProtocol

  case object Continue extends StreamProtocol

  case object Stop extends StreamProtocol

}

//TODO 了解Event类(actor是怎么收到Event的,它的两个参数值是从哪来的)
//TODO     个人理解:Event的第二个参数表示的是当前的状态数据. 而当前的状态数据可以使用using方法进行修改
//TODO 了解stateTimeout的作用
class ConnectorFSM[Key, Msg](props: AkkaConsumerProps[Key, Msg], connector: ConsumerConnector) extends Actor with FSM[ConnectorState, Int]{
  import props._
  import context.dispatcher

  startWith(Stopped, 0)

  var commitTimeoutCancelable: Option[Cancellable] = None
  var commiter: Option[ActorRef] = None

  def schedulerCommit = {
    commitTimeoutCancelable = commitConfig.commitInterval.map{i => context.system.scheduler.scheduleOnce(i, self, Commit)}
  }

  when(Stopped){
    case Event(Start, _) =>
      log.info("at=start")
      def startTopic(topic: String): Unit ={
        // TODO ConsumerConnector的createMessageStrerams方法
        connector.createMessageStreams(Map(topic -> streams), keyDecoder, msgDecoder).apply(topic).zipWithIndex.foreach{
          case (stream, index) => context.actorOf(Props(new StreamFSM(stream, maxInFlightPerStream, receiver, msgHandler)), s"stream$index")
        }
      }

      // TODO ConsumerConnector的createMessageStreamsByFilter方法
      def startTopicFilter(topicFilter: TopicFilter): Unit ={
        connector.createMessageStreamsByFilter(topicFilter, streams, keyDecoder, msgDecoder).zipWithIndex.foreach{
          case (stream, index) => context.actorOf(Props(new StreamFSM(stream, maxInFlightPerStream, receiver, msgHandler)), s"stream$index")
        }
      }

      topicFilterOrTopic.fold(startTopicFilter, startTopic)   // 创建KafkaStream,创建actorRef

      log.info("at=created-stream")
      context.children.foreach(_ ! Continue)    //向当前actor的所有子actor发送Continue消息
      schedulerCommit                           //定时向自身发送Commit消息
      sender ! Started                          //向sender发送Started消息
      goto(Receiving) using 0         //当前的FSM的状态转换为Receiving
  }

  when(Receiving){
    case Event(Received, uncommited) if commitConfig.commitAfterMsgCount.exists(_ == uncommited) =>  //当前FSM的状态转换为Committing
      debugRec(Received, uncommited)
      goto(Committing) using 0
    case Event(Received, uncommited) =>  //如果发生了Received事件,FSM维持当前状态
      debugRec(Received, uncommited + 1)
      stay using (uncommited + 1)
    case Event(Commit, uncommited) =>   //如果发生了Commit事件,FSM状态转换为Committing
      debugRec(Commit, uncommited)
      commiter = Some(sender)
      goto(Committing) using 0
    case Event(Committed, uncommitted) =>  //如果发生了Committed事件,FSM维持当前状态
      debugRec(Committed, uncommitted)
      stay
    case Event(d@Drained(s), uncommitted) =>   //如果发生了Drained事件,FSM维持当前状态
      debugRec(d, uncommitted)
      stay
  }

  when(Committing, stateTimeout = 1 seconds){
    case Event(Received, drained) =>         //如果发生了Received事件,FSM维持当前状态
      debugCommit(Received, "stream", drained)
      stay
    case Event(StateTimeout, drained) =>     //如果发生了StateTimeour事件,向当前actor的所有子actor发送Drain消息;FSM维持当前状态
      log.warning("state={} msg={} drained={} streams={}", Committing, StateTimeout, drained, streams)
      context.children.foreach(_ ! Drain)
      stay using 0
    // TODO Drained消息的作用?
    case Event(d@Drained(stream), drained) if drained + 1 < context.children.size =>
      debugCommit(d, stream, drained + 1)
      stay using (drained + 1)
    case Event(d@Drained(stream), drained) if drained + 1 == context.children.size =>
      debugCommit(d, stream, drained + 1)
      log.info("at=drain-finished")
      connector.commitOffsets
      log.info("at=commit-offsets")
      goto(Receiving) using 0
  }

  onTransition{
    case Receiving -> Committing =>
      // TODO FSM的stateData方法
      log.info("at=transition from={} to={} uncommitted={}", Receiving, Committing, stateData)
      commitTimeoutCancelable.foreach(_.cancel())
      context.children.foreach(_ ! Drain)
  }

  onTransition{
    case Committing -> Receiving =>
      log.info("at=transition from={} to={}", Committing, Receiving)
      schedulerCommit
      commiter.foreach(_ ! Committed)
      commiter = None
      context.children.foreach(_ ! StartProcessing)
  }

  whenUnhandled{
    case Event(ConnectorFSM.Stop, _) =>
      connector.shutdown()
      sender ! ConnectorFSM.Stop
      context.children.foreach(_ ! StreamFSM.Stop)
      stop()
  }

  def debugRec(msg: AnyRef, uncommited: Int) = log.debug("state={} msg={} uncommited={}", Receiving, msg, uncommited)

  def debugCommit(msg: AnyRef, stream: String, drained: Int) = log.debug("state={} msg={} drained={}", Committing, msg, drained)
}

class StreamFSM[Key, Msg](stream: KafkaStream[Key, Msg], maxOutstanding: Int, receiver: ActorRef, msgHandler: (MessageAndMetadata[Key, Msg]) => Any) extends Actor with FSM[StreamState, Int]{
  lazy val msgIterator = stream.iterator()

  val conn = context.parent

  def hasNext() = try{
    msgIterator.hasNext()
  }catch{
    case ex: ConsumerTimeoutException => false
  }

  startWith(Processing, 0)

  when(Processing){
    /* too many outstanding, wait */
    case Event(Continue, outstanding) if outstanding == maxOutstanding =>
      debug(Processing, Continue, outstanding)
      goto(Full)
    /* ok to process, and msg available */
    case Event(Continue, outstanding) if hasNext() =>
      val msg = msgHandler(msgIterator.next())
      conn ! Received
      debug(Processing, Continue, outstanding + 1)
      receiver ! msg
      self ! Continue
      stay using (outstanding + 1)
    /* no message in iterator and no outstanding. this stream is prob not going to get messages */
    case Event(Continue, outstanding) if outstanding == 0 =>
      debug(Processing, Continue, outstanding)
      goto(Unused)
    /* no msg in iterator, but have outstanding */
    case Event(Continue, outstanding) =>
      debug(Processing, Continue, outstanding)
      goto(FlattenContinue)
    /* message processed */
    case Event(Processed, outstanding) =>
      debug(Processing, Processed, outstanding + 1)
      self ! Continue
      stay using (outstanding + 1)
    /* conn says drain, we have no outstanding */
    case Event(Drain, outstanding) if outstanding == 0 =>
      debug(Processing, Drain, outstanding)
      goto(Empty)
    /* conn says drain, we have outstanding */
    case Event(Drain, outstanding) =>
      debug(Processing, Drain, outstanding)
      goto(Draining)
  }

  when(FlattenContinue){
    case Event(Continue, outstanding) =>
      debug(FlattenContinue, Continue, outstanding)
      stay
    case Event(Processed, outstanding) if outstanding == 1 =>
      debug(FlattenContinue, Processed, outstanding - 1)
      self ! Continue
      goto(Processing) using (outstanding - 1)
    case Event(Processed, outstanding) =>
      debug(FlattenContinue, Processed, outstanding - 1)
      stay using (outstanding - 1)
    case Event(Drain, outstanding) if outstanding == 0 =>
      debug(FlattenContinue, Drain, outstanding)
      goto(Empty)
    case Event(Drain, outstanding) =>
      debug(FlattenContinue, Drain, outstanding)
      goto(Draining)
  }

  when(Full){
    case Event(Continue, outstanding) =>
      debug(Full, Continue, outstanding)
      stay
    case Event(Processed, outstanding) =>
      debug(Full, Processed, outstanding - 1)
      goto(Processing) using (outstanding - 1)
    case Event(Drain, outstanding) =>
      debug(Full, Drain, outstanding)
      goto(Draining)
  }

  when(Draining){
    case Event(Processed, outstanding) if outstanding == 1 =>
      debug(Draining, Processed, outstanding)
      goto(Empty) using 0
    case Event(Processed, outstanding) =>
      debug(Draining, Processed, outstanding)
      stay using (outstanding - 1)
    case Event(Continue, outstanding) =>
      debug(Draining, Continue, outstanding)
      stay
    case Event(Drain, outstanding) =>
      debug(Draining, Drain, outstanding)
      stay
  }

  when(Empty){
    case Event(StartProcessing, outstanding) =>
      debug(Unused, Drain, outstanding)
      goto(Processing) using 0
    case Event(Continue, outstanding) =>
      debug(Unused, Drain, outstanding)
      stay
    case Event(Drain, _) =>
      conn ! Drained(me)
      stay
  }

  when(Unused){
    case Event(Drain, outstanding) =>
      debug(Unused, Drain, outstanding)
      goto(Empty)
    case Event(Continue, outstanding) =>
      debug(Unused, Continue, outstanding)
      goto(Processing)
  }

  onTransition{
    case Empty -> Processing => self ! Continue
  }

  onTransition{
    case Full -> Processing => self ! Continue
  }

  onTransition{
    case Processing -> Full => self ! Continue
  }

  onTransition{
    case _ -> Empty =>
      log.debug("stream={} at=Drained", me)
      conn ! Drained(me)
  }

  onTransition(handler _)

  whenUnhandled{
    case Event(StreamFSM.Stop, _) =>
      stop()
  }

  def handler(from: StreamState, to: StreamState) = {
    log.debug("stream={} at=transition from={} to={}", me, from, to)
  }

  def debug(state: StreamState, msg: StreamProtocol, outstanding: Int) = log.debug("stream={} state={} msg={} outstanding={}", me, state, msg, outstanding)

  lazy val me = self.path.name

  @scala.throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.error(reason, "message {}", message)
    super.preRestart(reason, message)
  }
}