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
  
  val receptionistPort = "4579"

  val initialContacts = Set(
    ActorPath.fromString("akka.tcp://ClusterSystem@127.0.0.1:4579/system/receptionist"))
  val settings = ClusterClientSettings(context.system)
    .withInitialContacts(initialContacts)

  val c = context.system.actorOf(ClusterClient.props(settings), "demo-client")


  def receive = {
    case TransformationResult(result) => {
      println("Client response")
      println(result)
    }
    case Send(counter) => {
      
        val job = new java.lang.String("PING")
        implicit val timeout = Timeout(5 seconds)
       val result = Patterns.ask(c,ClusterClient.Send("/user/clusterListener" + receptionistPort, new java.lang.String("PING"), localAffinity = true), timeout)

        result.onComplete {
          case Success(transformationResult) => {
            println(s"Client saw result: $transformationResult")
            self ! transformationResult
          }
          case Failure(t) => println("An error has occured: " + t.getMessage)
        }
      }
  }
}
