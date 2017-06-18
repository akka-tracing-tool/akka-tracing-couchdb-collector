package pl.edu.agh.iet.akka_tracing.couchdb.model

import java.util.UUID

import org.json4s.JValue
import pl.edu.agh.iet.akka_tracing.model.MessagesRelation

case class CouchDbReceiverMessage(_id: String,
                                  receiver: String,
                                  _rev: Option[String] = None)
  extends Document

case class CouchDbSenderMessage(_id: String,
                                sender: String,
                                contents: Option[JValue],
                                _rev: Option[String] = None)
  extends Document

case class CouchDbMessagesRelation(_id: String,
                                   id1: String,
                                   id2: String,
                                   _rev: Option[String] = None)
  extends Document {

  def toMessagesRelation: MessagesRelation =
    MessagesRelation(UUID.fromString(id1), UUID.fromString(id2))
}
