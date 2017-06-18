package pl.edu.agh.iet.akka_tracing.collector

import java.util.concurrent.{ Executors, TimeUnit }

import com.typesafe.config.Config
import pl.edu.agh.iet.akka_tracing.couchdb.model._
import pl.edu.agh.iet.akka_tracing.model.{ MessagesRelation, ReceiverMessage, SenderMessage }
import pl.edu.agh.iet.akka_tracing.utils.DatabaseUtils

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class CouchDbCollector(config: Config)
                      (implicit val ec: ExecutionContext)
  extends Collector {

  private[akka_tracing] val databaseUtils = new DatabaseUtils(config)

  import databaseUtils._

  private val senderMessagesQueue = mutable.MutableList[CouchDbSenderMessage]()
  private val receiverMessagesQueue = mutable.MutableList[CouchDbReceiverMessage]()
  private val messagesRelationsQueue = mutable.MutableList[CouchDbMessagesRelation]()

  private val threadPool = Executors.newScheduledThreadPool(1)
  threadPool.scheduleAtFixedRate(new Runnable {
    def run(): Unit = {
      senderMessagesQueue.synchronized {
        val senderMessages = senderMessagesQueue.toList
        senderMessagesQueue.clear()
        senderMessagesDatabase.putDocs(senderMessages)
      }
      receiverMessagesQueue.synchronized {
        val receiverMessages = receiverMessagesQueue.toList
        receiverMessagesQueue.clear()
        receiverMessagesDatabase.putDocs(receiverMessages)
      }
      messagesRelationsQueue.synchronized {
        val messagesRelations = messagesRelationsQueue.toList
        messagesRelationsQueue.clear()
        messagesRelationsDatabase.putDocs(messagesRelations)
      }
    }
  }, 500, 500, TimeUnit.MILLISECONDS)

  override def handleSenderMessage(msg: SenderMessage): Unit = {
    senderMessagesQueue.synchronized {
      senderMessagesQueue += CouchDbSenderMessage(msg.id.toString, msg.sender, msg.contents)
    }
  }

  override def handleReceiverMessage(msg: ReceiverMessage): Unit = {
    receiverMessagesQueue.synchronized {
      receiverMessagesQueue += CouchDbReceiverMessage(msg.id.toString, msg.receiver)
    }
  }

  override def handleRelationMessage(msg: MessagesRelation): Unit = {
    val id1 = msg.id1.toString
    val id2 = msg.id2.toString
    messagesRelationsQueue.synchronized {
      messagesRelationsQueue += CouchDbMessagesRelation(s"${id1}_$id2", id1, id2)
    }
  }
}

class CouchDbCollectorConstructor extends CollectorConstructor {
  override def fromConfig(config: Config)(implicit ec: ExecutionContext): Collector = {
    new CouchDbCollector(config)
  }
}
