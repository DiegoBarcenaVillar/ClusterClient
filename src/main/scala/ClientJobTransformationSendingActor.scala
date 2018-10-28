import akka.actor.Actor
import akka.actor.ActorPath
import akka.cluster.client.{ClusterClient, ClusterClientSettings}
import akka.pattern.Patterns
import sample.cluster.transformation.{TransformationJob, TransformationResult}
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

import spray.json._
import DefaultJsonProtocol._

class ClientJobTransformationSendingActor extends Actor {

  val receptionistPort = "5000"

  val initialContacts = Set(
    ActorPath.fromString("akka.tcp://ClusterSystem@127.0.0.1:".concat(receptionistPort)
      .concat("/system/receptionist")))

  val settings = ClusterClientSettings(context.system)
    .withInitialContacts(initialContacts)

  val c = context.system.actorOf(ClusterClient.props(settings), "demo-client")

  implicit val timeout = Timeout(5 seconds)

  def receive = {

    case msg:String =>{
      println(s"Client saw result: $msg")
      println("")
      processStringJson(msg)
    }

    case SendString(query) =>{
      processSend(query)
    }

    case SendInt(counter) => {
      processSend ("PING")
    }
  }

  def processSend (text:String ) :  Unit= {

    val result = Patterns.ask(c,ClusterClient.Send("/user/clusterListener", text, localAffinity = true), timeout)

    result.onComplete {
      case Success(transformationResult) => {
        self ! transformationResult
      }
      case Failure(t) => println("An error has occured: " + t.getMessage)
    }
  }

  def processStringJson(source:String):Unit = {

    val listOfRows = source.split(";")

    listOfRows.foreach(t => println(t.parseJson))

    val itemJson = listOfRows.map(t => t.parseJson.convertTo[Map[JsValue, JsValue]])

    var i : Int = 0

    var cabecera:String = ""
    var fila:String = " "

    while(i<itemJson.size)
    {
      if(i==0)
      {
        itemJson(i).foreach(
          x =>
          {
            cabecera += x._1 + " "
            fila += x._2 + "   "
          }
        )
        println(cabecera)
        println(fila)
      }
      else
      {
        fila = ""
        itemJson(i).foreach(
          x =>
          {
            fila += x._2 + "  "
          }
        )
        println(fila)
      }
      i = i+1
    }
  }
}
