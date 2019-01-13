import akka.actor.ActorPath
import akka.util.Timeout
import scala.concurrent.duration._
//import scala.concurrent.ExecutionContext.Implicits.global

object Constants {

  val receptionistPorts = Seq("5000", "2551", "2552")
  val initialContacts = Set(
    ActorPath.fromString("akka.tcp://ClusterSystem@127.0.0.1:".concat(receptionistPorts(0))
      .concat("/system/receptionist"))
    , ActorPath.fromString("akka.tcp://ClusterSystem@127.0.0.1:".concat(receptionistPorts(1))
      .concat("/system/receptionist"))
    , ActorPath.fromString("akka.tcp://ClusterSystem@127.0.0.1:".concat(receptionistPorts(2))
      .concat("/system/receptionist")))

  implicit val timeout: Timeout = Timeout(20 seconds)


}
