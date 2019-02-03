package tfm

import akka.actor.{ActorRef, ActorSystem, Props}
import tfm.common.Constants.waitingForResponseFromConsoleTimeOut
import scala.io.StdIn

object TfmClient {

  def main(args : Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\winutils_hadoop2.6.0\\")

    val system = ActorSystem("TfmSystem")

    val clientJobTransformationSendingActor : ActorRef = system.actorOf(Props[ClientJobTransformationSendingActor],
        name = "clientJobTransformationSendingActor")

    sendingMessagesToClusterServer(clientJobTransformationSendingActor)

    runConsole(clientJobTransformationSendingActor)

    system.terminate()
  }

  def sendingMessagesToClusterServer(clientJobTransformationSendingActor : ActorRef) : Unit = {

    clientJobTransformationSendingActor ! SendString("GET COLUMNS PARQUET")

    clientJobTransformationSendingActor ! SendString("GET COLUMNS CSV")
  }

  def runConsole(clientJobTransformationSendingActor : ActorRef) : Unit ={

    var sqlQuery: String = null.asInstanceOf[String]

    do {
      Thread.currentThread().join(waitingForResponseFromConsoleTimeOut*1000l)

      println("Enter your SQL Query or EXIT to exit the Session:")

      sqlQuery = StdIn.readLine()

      if (!sqlQuery.equalsIgnoreCase("EXIT"))
        clientJobTransformationSendingActor ! SendString(sqlQuery)

      println(s"Your Query is: $sqlQuery")

    }while(!sqlQuery.equalsIgnoreCase("EXIT"))
  }
}
