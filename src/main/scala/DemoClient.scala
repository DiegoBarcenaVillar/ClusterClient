import akka.actor.{ActorRef, ActorSystem, Props}
import scala.io.StdIn

object DemoClient {
  def main(args : Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\winutils_hadoop2.6.0\\");

    val system = ActorSystem("OtherSystem")
    var clientJobTransformationSendingActor: ActorRef = null

    clientJobTransformationSendingActor = system.actorOf(Props[ClientJobTransformationSendingActor],
                                        name = "clientJobTransformationSendingActor")

    clientJobTransformationSendingActor ! SendString("GET COLUMNS PARQUET")

    clientJobTransformationSendingActor ! SendString("GET COLUMNS CSV")

    var sqlQuery: String = null

    do {
      Thread.currentThread().join(5*1000l)
      println("Enter your SQL Query or EXIT to exit the Session:")
      sqlQuery = StdIn.readLine()

      if (!sqlQuery.equalsIgnoreCase("EXIT"))
        clientJobTransformationSendingActor ! SendString(sqlQuery)

      println(s"Your Query is: $sqlQuery")

    }while(!sqlQuery.equalsIgnoreCase("EXIT"))
    system.terminate()
  }
}
