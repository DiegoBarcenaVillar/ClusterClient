import akka.actor.Actor
import akka.actor.ActorPath
import akka.cluster.client.{ClusterClientSettings, ClusterClient}
import akka.pattern.Patterns
import sample.cluster.transformation.{TransformationResult, TransformationJob}
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

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
}
