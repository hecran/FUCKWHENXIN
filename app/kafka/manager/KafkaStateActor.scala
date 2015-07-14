/**
 * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

package kafka.manager

import java.text.SimpleDateFormat

import com.google.common.primitives.Longs
import kafka.api.{PartitionOffsetRequestInfo, OffsetRequest}
import kafka.common.BrokerNotAvailableException
import kafka.consumer.SimpleConsumer
import kafka.manager.utils.zero81.{ReassignPartitionCommand, PreferredReplicaLeaderElectionCommand}
import kafka.utils.{Json, ZKStringSerializer}
import org.I0Itec.zkclient.ZkClient
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode
import org.apache.curator.framework.recipes.cache._
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.data.Stat
import org.joda.time.{DateTimeZone, DateTime}
import kafka.manager.utils.{TopicAndPartition, ZkUtils}

import scala.collection.{Map, Seq, immutable, mutable}
import scala.util.control.NonFatal
import scala.util.{Success, Failure, Try}
import scala.collection.JavaConversions._


/**
 * @author hiral
 */

import ActorModel._
import kafka.manager.utils._
import scala.collection.JavaConverters._

class KafkaStateActor(curator: CuratorFramework,
                      deleteSupported: Boolean,
                      clusterConfig: ClusterConfig) extends BaseQueryCommandActor {

  // e.g. /brokers/topics/analytics_content/partitions/0/state
  private[this] val groupCache = new TreeCache(curator, ZkUtils.ConsumersPath)

  private[this] val zkClient = new ZkClient(clusterConfig.curatorConfig.zkConnect, 30000, 5000, ZKStringSerializer)

  private val consumerMap: mutable.Map[Int, Option[SimpleConsumer]] = mutable.Map()

  private[this] val topicsTreeCache = new TreeCache(curator, ZkUtils.BrokerTopicsPath)

  private[this] val topicsConfigPathCache = new PathChildrenCache(curator, ZkUtils.TopicConfigPath, true)

  private[this] val brokersPathCache = new PathChildrenCache(curator, ZkUtils.BrokerIdsPath, true)

  private[this] val adminPathCache = new PathChildrenCache(curator, ZkUtils.AdminPath, true)

  private[this] val deleteTopicsPathCache = new PathChildrenCache(curator, ZkUtils.DeleteTopicsPath, true)

  @volatile
  private[this] var topicsTreeCacheLastUpdateMillis: Long = System.currentTimeMillis()

  private[this] val topicsTreeCacheListener = new TreeCacheListener {
    override def childEvent(client: CuratorFramework, event: TreeCacheEvent): Unit = {
      event.getType match {
        case TreeCacheEvent.Type.INITIALIZED | TreeCacheEvent.Type.NODE_ADDED |
             TreeCacheEvent.Type.NODE_REMOVED | TreeCacheEvent.Type.NODE_UPDATED =>
          topicsTreeCacheLastUpdateMillis = System.currentTimeMillis()
        case _ =>
        //do nothing
      }
    }
  }

  @volatile
  private[this] var preferredLeaderElection: Option[PreferredReplicaElection] = None

  @volatile
  private[this] var reassignPartitions: Option[ReassignPartitions] = None

  private[this] val adminPathCacheListener = new PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      log.info(s"Got event : ${event.getType} path=${Option(event.getData).map(_.getPath)}")
      event.getType match {
        case PathChildrenCacheEvent.Type.INITIALIZED =>
          event.getInitialData.asScala.foreach { cd: ChildData =>
            updatePreferredLeaderElection(cd)
            updateReassignPartition(cd)
          }
        case PathChildrenCacheEvent.Type.CHILD_ADDED | PathChildrenCacheEvent.Type.CHILD_UPDATED =>
          updatePreferredLeaderElection(event.getData)
          updateReassignPartition(event.getData)
        case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
          endPreferredLeaderElection(event.getData)
          endReassignPartition(event.getData)
        case _ =>
        //do nothing
      }
    }

    private[this] def updatePreferredLeaderElection(cd: ChildData): Unit = {
      if (cd != null && cd.getPath.endsWith(ZkUtils.PreferredReplicaLeaderElectionPath)) {
        Try {
          self ! KSUpdatePreferredLeaderElection(cd.getStat.getMtime, cd.getData)
        }
      }
    }

    private[this] def updateReassignPartition(cd: ChildData): Unit = {
      if (cd != null && cd.getPath.endsWith(ZkUtils.ReassignPartitionsPath)) {
        Try {
          self ! KSUpdateReassignPartition(cd.getStat.getMtime, cd.getData)
        }
      }
    }

    private[this] def endPreferredLeaderElection(cd: ChildData): Unit = {
      if (cd != null && cd.getPath.endsWith(ZkUtils.PreferredReplicaLeaderElectionPath)) {
        Try {
          self ! KSEndPreferredLeaderElection(cd.getStat.getMtime)
        }
      }
    }

    private[this] def endReassignPartition(cd: ChildData): Unit = {
      if (cd != null && cd.getPath.endsWith(ZkUtils.ReassignPartitionsPath)) {
        Try {
          self ! KSEndReassignPartition(cd.getStat.getMtime)
        }
      }
    }
  }

  @scala.throws[Exception](classOf[Exception])
  override def preStart() = {
    log.info("Started actor %s".format(self.path))
    log.info("Starting topics tree cache...")
    topicsTreeCache.start()
    log.info("Starting topics config path cache...")
    topicsConfigPathCache.start(StartMode.BUILD_INITIAL_CACHE)
    log.info("Starting brokers path cache...")
    brokersPathCache.start(StartMode.BUILD_INITIAL_CACHE)
    log.info("Starting admin path cache...")
    adminPathCache.start(StartMode.BUILD_INITIAL_CACHE)
    log.info("Starting delete topics path cache...")
    deleteTopicsPathCache.start(StartMode.BUILD_INITIAL_CACHE)

    log.info("Adding topics tree cache listener...")
    topicsTreeCache.getListenable.addListener(topicsTreeCacheListener)
    log.info("Adding admin path cache listener...")
    adminPathCache.getListenable.addListener(adminPathCacheListener)
    groupCache.start()
  }

  @scala.throws[Exception](classOf[Exception])
  override def preRestart(reason: Throwable, message: Option[Any]) {
    log.error(reason, "Restarting due to [{}] when processing [{}]",
      reason.getMessage, message.getOrElse(""))
    super.preRestart(reason, message)
  }


  @scala.throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    log.info("Stopped actor %s".format(self.path))

    log.info("Removing admin path cache listener...")
    Try(adminPathCache.getListenable.removeListener(adminPathCacheListener))
    log.info("Removing topics tree cache listener...")
    Try(topicsTreeCache.getListenable.removeListener(topicsTreeCacheListener))

    log.info("Shutting down delete topics path cache...")
    Try(deleteTopicsPathCache.close())
    log.info("Shutting down admin path cache...")
    Try(adminPathCache.close())
    log.info("Shutting down brokers path cache...")
    Try(brokersPathCache.close())
    log.info("Shutting down topics config path cache...")
    Try(topicsConfigPathCache.close())
    log.info("Shutting down topics tree cache...")
    Try(topicsTreeCache.close())
    Try(groupCache.close())

    super.postStop()
  }

  def getTopicDescription(topic: String): Option[TopicDescription] = {
    val topicPath = "%s/%s".format(ZkUtils.BrokerTopicsPath, topic)
    val descriptionOption: Option[(Int, String)] =
      Option(topicsTreeCache.getCurrentData(topicPath)).map(childData => (childData.getStat.getVersion, asString(childData.getData)))

    for {
      description <- descriptionOption
      partitionsPath = "%s/%s/partitions".format(ZkUtils.BrokerTopicsPath, topic)
      partitions: Map[String, ChildData] <- Option(topicsTreeCache.getCurrentChildren(partitionsPath)).map(_.asScala.toMap)
      states: Map[String, String] = partitions flatMap { case (part, _) =>
        val statePath = s"$partitionsPath/$part/state"
        Option(topicsTreeCache.getCurrentData(statePath)).map(cd => (part, asString(cd.getData)))
      }
      config = getTopicConfigString(topic)
    } yield TopicDescription(topic, description, Option(states.toMap), config, deleteSupported)
  }

  def getGroups() = {
    val groupPath = "%s".format(ZkUtils.ConsumersPath)
    val groups = groupCache.getCurrentChildren(groupPath)
    val tmp = groups.keySet().toIndexedSeq
    GroupList(tmp)
  }

  def getOffsets(clusterName: String, groupName: String) = {
    val offsetTopicPath = "%s/%s/offsets".format(ZkUtils.ConsumersPath, groupName)
   val result= groupCache.getCurrentChildren(offsetTopicPath).map {
      case (k, v) =>
        processTopic(groupName, k)
    }
    OffsetList(result.toIndexedSeq)
  }

  private def getConsumer(bid: Int): Option[SimpleConsumer] = {
    try {
      kafka.utils.ZkUtils.readDataMaybeNull(zkClient, kafka.utils.ZkUtils.BrokerIdsPath + "/" + bid) match {
        case (Some(brokerInfoString), _) =>
          Json.parseFull(brokerInfoString) match {
            case Some(m) =>
              val brokerInfo = m.asInstanceOf[Map[String, Any]]
              val host = brokerInfo.get("host").get.asInstanceOf[String]
              val port = brokerInfo.get("port").get.asInstanceOf[Int]
              Some(new SimpleConsumer(host, port, 10000, 100000, "ConsumerOffsetChecker"))
            case None =>
              throw new BrokerNotAvailableException("Broker id %d does not exist".format(bid))
          }
        case (None, _) =>
          throw new BrokerNotAvailableException("Broker id %d does not exist".format(bid))
      }
    } catch {
      case t: Throwable =>
        log.error("Could not parse broker info", t)
        None
    }
  }

  private def processPartition(group: String, topic: String, pid: Int): Option[PartitionConsumerOffsetIdentity] = {
    try {
      val (offset, stat: Stat) = kafka.utils.ZkUtils.readData(zkClient, s"${kafka.utils.ZkUtils.ConsumersPath}/$group/offsets/$topic/$pid")
      val (owner, _) = kafka.utils.ZkUtils.readDataMaybeNull(zkClient, s"${kafka.utils.ZkUtils.ConsumersPath}/$group/owners/$topic/$pid")

      kafka.utils.ZkUtils.getLeaderForPartition(zkClient, topic, pid) match {
        case Some(bid) =>
          val consumerOpt = consumerMap.getOrElseUpdate(bid, getConsumer(bid))
          consumerOpt map {
            consumer =>
              val topicAndPartition = kafka.common.TopicAndPartition(topic, pid)
              val request =
                OffsetRequest(immutable.Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
              val logSize = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets.head
              val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

              PartitionConsumerOffsetIdentity(pid,
                offset.toLong,
                logSize,
                logSize - offset.toLong,
                owner.get,
                sdf.format(stat.getCtime),
                sdf.format(stat.getMtime))
          }
        case None =>
          log.error("No broker for partition %s - %s".format(topic, pid))
          None
      }
    } catch {
      case NonFatal(t) =>
        log.error(s"Could not parse partition info. group: [$group] topic: [$topic]", t)
        None
    }
  }

  private def processTopic(group: String, topic: String): TopicConsumerOffsetIdentity = {
    val pidMap = kafka.utils.ZkUtils.getPartitionsForTopics(zkClient, Seq(topic))
    val list = for {
      partitions <- pidMap.get(topic).toSeq
      pid <- partitions.sorted
      info <- processPartition(group, topic, pid)
    } yield info
    val offset = list.foldLeft(0L)(_+_.offset)
    val logSize = list.foldLeft(0L)(_+_.logSize)
    val lag = list.foldLeft(0L)(_+_.lag)
    TopicConsumerOffsetIdentity(topic, offset, logSize, lag, list.toIndexedSeq)
  }

  def getPartitionConsumerOffsetIdentity(topicPath: String, topic: String): TopicConsumerOffsetIdentity = {
    val partitionOffsetsMap = groupCache.getCurrentChildren(topicPath).asScala
    val offsetData = partitionOffsetsMap.map{
      case (partNum, _) =>
        val partPath = s"$topicPath/$partNum"
        Option(groupCache.getCurrentData(partPath)).map(cd => (partNum.toString, cd))
    }
    val result = offsetData.map(x => (x.get._1, x.get._2)).toMap
//    val offset = result.foldLeft(0L)(_+_._2.getData)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    val list = result.map {
      case (partNum, childData) =>
        val b = new Array[Byte](8-childData.getData.size)
        PartitionConsumerOffsetIdentity(
        partNum.toInt, Longs.fromByteArray(b+childData.getData), 0, 0, "owner",
        sdf.format(childData.getStat.getCtime),
        sdf.format(childData.getStat.getMtime))
    }.toIndexedSeq
    TopicConsumerOffsetIdentity(topic, 0, 0, 0, list)
  }


  override def processActorResponse(response: ActorResponse): Unit = {
    response match {
      case any: Any => log.warning("ksa : processActorResponse : Received unknown message: {}", any.toString)
    }
  }

  private[this] def getTopicConfigString(topic: String): Option[(Int, String)] = {
    val data: mutable.Buffer[ChildData] = topicsConfigPathCache.getCurrentData.asScala
    val result: Option[ChildData] = data.find(p => p.getPath.endsWith(topic))
    result.map(cd => (cd.getStat.getVersion, asString(cd.getData)))
  }

  override def processQueryRequest(request: QueryRequest): Unit = {
    request match {
      case KSGetTopics =>
        val deleteSet: Set[String] = {
          if (deleteSupported) {
            val deleteTopicsData: mutable.Buffer[ChildData] = deleteTopicsPathCache.getCurrentData.asScala
            deleteTopicsData.map { cd =>
              nodeFromPath(cd.getPath)
            }.toSet
          } else {
            Set.empty
          }
        }
        withTopicsTreeCache { cache =>
          cache.getCurrentChildren(ZkUtils.BrokerTopicsPath)
        }.fold {
          sender ! TopicList(IndexedSeq.empty, deleteSet)
        } { data: java.util.Map[String, ChildData] =>
          sender ! TopicList(data.asScala.map(kv => kv._1).toIndexedSeq, deleteSet)
        }

      case KSGetTopicConfig(topic) =>
        sender ! TopicConfig(topic, getTopicConfigString(topic))

      case KSGetTopicDescription(topic) =>
        sender ! getTopicDescription(topic)

      case KSGetTopicDescriptions(topics) =>
        sender ! TopicDescriptions(topics.toIndexedSeq.map(getTopicDescription).flatten, topicsTreeCacheLastUpdateMillis)

      case KSGetAllTopicDescriptions(lastUpdateMillisOption) =>
        val lastUpdateMillis = lastUpdateMillisOption.getOrElse(0L)
        if (topicsTreeCacheLastUpdateMillis > lastUpdateMillis) {
          //we have option here since there may be no topics at all!
          withTopicsTreeCache { cache: TreeCache =>
            cache.getCurrentChildren(ZkUtils.BrokerTopicsPath)
          }.fold {
            sender ! TopicDescriptions(IndexedSeq.empty, topicsTreeCacheLastUpdateMillis)
          } { data: java.util.Map[String, ChildData] =>
            sender ! TopicDescriptions(data.asScala.keys.toIndexedSeq.map(getTopicDescription).flatten, topicsTreeCacheLastUpdateMillis)
          }
        } // else no updates to send

      case KSGetTopicsLastUpdateMillis =>
        sender ! topicsTreeCacheLastUpdateMillis

      case KSGetBrokers =>
        val data: mutable.Buffer[ChildData] = brokersPathCache.getCurrentData.asScala
        val result: IndexedSeq[BrokerIdentity] = data.map { cd =>
          BrokerIdentity.from(nodeFromPath(cd.getPath).toInt, asString(cd.getData))
        }.filter { v =>
          v match {
            case scalaz.Failure(nel) =>
              log.error(s"Failed to parse broker config $nel")
              false
            case _ => true
          }
        }.collect {
          case scalaz.Success(bi) => bi
        }.toIndexedSeq.sortBy(_.id)
        sender ! BrokerList(result, clusterConfig)

      case KSGetPreferredLeaderElection =>
        sender ! preferredLeaderElection

      case KSGetReassignPartition =>
        sender ! reassignPartitions

      case KSGetGroups =>
        sender ! getGroups
      case KSGetOffsets(clusterName, groupName) =>
        sender ! getOffsets(clusterName, groupName)

      case any: Any => log.warning("ksa : processQueryRequest : Received unknown message: {}", any.toString)
    }
  }

  override def processCommandRequest(request: CommandRequest): Unit = {
    request match {
      case KSUpdatePreferredLeaderElection(millis, json) =>
        safeExecute {
          val s: Set[TopicAndPartition] = PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(json)
          preferredLeaderElection.fold {
            //nothing there, add as new
            preferredLeaderElection = Some(PreferredReplicaElection(getDateTime(millis), s, None))
          } {
            existing =>
              existing.endTime.fold {
                //update without end? Odd, copy existing
                preferredLeaderElection = Some(existing.copy(topicAndPartition = existing.topicAndPartition ++ s))
              } { _ =>
                //new op started
                preferredLeaderElection = Some(PreferredReplicaElection(getDateTime(millis), s, None))
              }
          }
        }
      case KSUpdateReassignPartition(millis, json) =>
        safeExecute {
          val m: Map[TopicAndPartition, Seq[Int]] = ReassignPartitionCommand.parsePartitionReassignmentZkData(json)
          reassignPartitions.fold {
            //nothing there, add as new
            reassignPartitions = Some(ReassignPartitions(getDateTime(millis), m.toMap, None))
          } {
            existing =>
              existing.endTime.fold {
                //update without end? Odd, copy existing
                reassignPartitions = Some(existing.copy(partitionsToBeReassigned = existing.partitionsToBeReassigned ++ m))
              } { _ =>
                //new op started
                reassignPartitions = Some(ReassignPartitions(getDateTime(millis), m.toMap, None))
              }
          }
        }
      case KSEndPreferredLeaderElection(millis) =>
        safeExecute {
          preferredLeaderElection.foreach { existing =>
            preferredLeaderElection = Some(existing.copy(endTime = Some(getDateTime(millis))))
          }
        }
      case KSEndReassignPartition(millis) =>
        safeExecute {
          reassignPartitions.foreach { existing =>
            reassignPartitions = Some(existing.copy(endTime = Some(getDateTime(millis))))
          }
        }
      case any: Any => log.warning("ksa : processCommandRequest : Received unknown message: {}", any.toString)
    }
  }

  private[this] def getDateTime(millis: Long): DateTime = new DateTime(millis, DateTimeZone.UTC)

  private[this] def safeExecute(fn: => Any): Unit = {
    Try(fn) match {
      case Failure(t) =>
        log.error("Failed!", t)
      case Success(_) =>
      //do nothing
    }
  }

  private[this] def withTopicsTreeCache[T](fn: TreeCache => T): Option[T] = {
    Option(fn(topicsTreeCache))
  }

}

