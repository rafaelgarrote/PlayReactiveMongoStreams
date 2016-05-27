package provider.reactiveMongodb

import akka.actor.ActorRef
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson.BSONDocument

/**
  *
  */
object DataLifeCycle {

  def getSource(manager: DriverManager, collection: BSONCollection, query: BSONDocument): Source[String, Unit] = {

    def execute(actorRef: ActorRef) = manager.stream(collection, query, actorRef)

    Source.actorRef[String](bufferSize = 0, OverflowStrategy.fail).mapMaterializedValue(execute)
  }

  def getTailableSource(manager: DriverManager, collection: BSONCollection, query: BSONDocument): Source[String, Unit] = {

    def execute(actorRef: ActorRef) = manager.tailableStream(collection, query, actorRef)

    Source.actorRef[String](bufferSize = 0, OverflowStrategy.fail).mapMaterializedValue(execute)
  }

}
