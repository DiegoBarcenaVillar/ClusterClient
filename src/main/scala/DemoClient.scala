
import akka.actor.{ActorRef, ActorSystem, Props}
import scala.io.StdIn
//import scala.concurrent.ExecutionContext
//import scala.concurrent.Future
//import ExecutionContext.Implicits.global
//import scala.util.{Success, Failure}

object DemoClient {
  def main(args : Array[String]) {

    val system = ActorSystem("OtherSystem")
    var clientJobTransformationSendingActor: ActorRef = null

    clientJobTransformationSendingActor = system.actorOf(Props[ClientJobTransformationSendingActor],
                                        name = "clientJobTransformationSendingActor")

//    val futureActorRef: Future[ActorRef] = Future {
//      system.actorOf(Props[ClientJobTransformationSendingActor],
//        name = "clientJobTransformationSendingActor")
//
//    }
//
//
//    futureActorRef onComplete {
//      case Success(futureActorRefResult) => clientJobTransformationSendingActor = futureActorRefResult
//      case Failure(t) => println("Could not process file: " + t.getMessage)
//    }

    var sqlQuery: String = null

    do {
      Thread.currentThread().join(1000l)
      println("Enter your SQL Query or EXIT to exit the Session:")
      sqlQuery = StdIn.readLine()

      println(s"Your Query is: $sqlQuery")

      if (!sqlQuery.equalsIgnoreCase("EXIT"))
        clientJobTransformationSendingActor ! SendString(sqlQuery)

    }while(!sqlQuery.equalsIgnoreCase("EXIT"))

    system.terminate()
  }
}




