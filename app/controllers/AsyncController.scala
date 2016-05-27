package controllers

import javax.inject._

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.stream.ActorMaterializer
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import play.api.libs.streams.ActorFlow
import play.api.mvc._
import provider.properties.Host
import provider.properties.MongodbProperties
import provider.reactiveMongodb.DataLifeCycle
import provider.reactiveMongodb.DriverManager
import reactivemongo.bson.BSONDocument

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
 * This controller creates an `Action` that demonstrates how to write
 * simple asynchronous code in a controller. It uses a timer to
 * asynchronously delay sending a response for 1 second.
 *
 * @param actorSystem We need the `ActorSystem`'s `Scheduler` to
 * run code after a delay.
 * @param exec We need an `ExecutionContext` to execute our
 * asynchronous code.
 */
@Singleton
class AsyncController @Inject() (actorSystem: ActorSystem, materializer: Materializer)(implicit exec: ExecutionContext) extends Controller {

  implicit val system = actorSystem
  implicit val timeout = Timeout(1000.seconds)
  implicit val mat = materializer

  def message = WebSocket.accept[String, String] { request =>
    ActorFlow.actorRef(out => MyWebSocketActor.props(out))
  }
}

object MyWebSocketActor {
  def props(out: ActorRef) = Props(new MyWebSocketActor(out))
}

class MyWebSocketActor(out: ActorRef) extends Actor {
  implicit val flowMaterializer = ActorMaterializer()
  val hosts = List(new Host("127.0.0.1", 27017))
  val properties = new MongodbProperties(hosts, "zips", "zips")
  val manager: DriverManager = new DriverManager
  val sink = Sink.actorRef[String](out, onCompleteMessage = "stream completed")
  val size: Int = 10000

  def receive = {
    case msg: String =>
      val query: BSONDocument = BSONDocument()
      msg match {
        case "reactive" =>
          for{
            collection <- manager.connect(properties)
            isCapable <- collection.convertToCapped(size, None)
          } yield DataLifeCycle.getTailableSource(manager, collection, query) to sink run()
        case _ =>
          for{
            collection <- manager.connect(properties)
          } yield DataLifeCycle.getSource(manager, collection, query) to sink run()
      }
  }
}
