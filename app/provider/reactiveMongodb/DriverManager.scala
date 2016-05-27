package provider.reactiveMongodb

import akka.actor.ActorRef
import akka.util.Timeout
import provider.properties.MongodbProperties
import reactivemongo.api.Cursor
import reactivemongo.api.MongoDriver
import reactivemongo.api.QueryOpts
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.bson.BSONDocument

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

/**
  *
  */
class DriverManager {

  implicit val timeout = Timeout(100.seconds)
  val size: Int = 10000

  def connect(properties: MongodbProperties): Future[BSONCollection] = {
    // gets an instance of the driver
    // (creates an actor system)
    val driver = MongoDriver()
    val hostsList = properties.hosts.map(h => s"${h.name}:${h.port}")
    val connection = driver.connection(hostsList)

    // Gets a reference to the database "plugin"
    val db = connection.database(properties.database)

    // Gets a reference to the collection "acoll"
    // By default, you get a Future[BSONCollection].
    db.map(_(properties.collection))
  }


  def tailableStream(collection: BSONCollection, query: BSONDocument, actor: ActorRef): Unit = {
    collection.find(query).options(QueryOpts().tailable.awaitData).cursor[BSONDocument]().foldWhile(List.empty[String], 1000 /* optional: max doc */)(
      { (ls, str) => // process next String value
        if (BSONDocument.pretty(str) startsWith "#") Cursor.Cont(ls) // Skip: continue unchanged `ls`
        else if (BSONDocument.pretty(str) == "_end") Cursor.Done(ls) // End processing
        else {
          println("message send")
          actor ! BSONDocument.pretty(str)
          Cursor.Cont(BSONDocument.pretty(str) :: ls) // Continue with updated `ls`
        }
      }, { (ls, err) => // handle failure
        err match {
          case e: RuntimeException => Cursor.Cont(ls) // Skip error, continue
          case _ => Cursor.Fail(err) // Stop with current failure -> Future.failed
        }
      })
  }

  def stream(collection: BSONCollection, query: BSONDocument, actor: ActorRef): Unit = {
    collection.find(query).cursor[BSONDocument]().foldWhile(List.empty[String], 1000 /* optional: max doc */)(
      { (ls, str) => // process next String value
        if (BSONDocument.pretty(str) startsWith "#") Cursor.Cont(ls) // Skip: continue unchanged `ls`
        else if (BSONDocument.pretty(str) == "_end") Cursor.Done(ls) // End processing
        else {
          println("message send")
          actor ! BSONDocument.pretty(str)
          Cursor.Cont(BSONDocument.pretty(str) :: ls) // Continue with updated `ls`
        }
      }, { (ls, err) => // handle failure
        err match {
          case e: RuntimeException => Cursor.Cont(ls) // Skip error, continue
          case _ => Cursor.Fail(err) // Stop with current failure -> Future.failed
        }
      })
  }


}

