package tfm.common

import java.util.concurrent.TimeUnit

import akka.actor.ActorPath
import akka.util.Timeout

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config


import scala.concurrent.duration.FiniteDuration

object Constants {

  val tfmConfig : Config = ConfigFactory.parseResources("tfm.conf")

  val responseFromServerTimeOut : Long = tfmConfig.getLong("responseFromServerTimeOut")
  val waitingForResponseFromConsoleTimeOut : Integer = tfmConfig.getInt("waitingForResponseFromConsoleTimeOut")

  val receptionistPorts : Seq[String] = tfmConfig.getStringList("serverReceptionPorts").toArray
    .map(t=>t.toString).toSeq
  val host1 : String = tfmConfig.getString("host1")
  val host2 : String = tfmConfig.getString("host2")
  val host3 : String = tfmConfig.getString("host3")

  val initialContacts = Set(
      ActorPath.fromString("akka.tcp://ClusterSystem@".concat(host1).concat(":").concat(receptionistPorts(0))
      .concat("/system/receptionist"))
    , ActorPath.fromString("akka.tcp://ClusterSystem@".concat(host2).concat(":").concat(receptionistPorts(1))
      .concat("/system/receptionist"))
    , ActorPath.fromString("akka.tcp://ClusterSystem@".concat(host3).concat(":").concat(receptionistPorts(2))
      .concat("/system/receptionist")))

  //implicit val timeout: Timeout = Timeout(20 seconds)

  //val receptionistPorts = Seq("5000", "2551", "2552")
  //  val initialContacts = Set(
  //    ActorPath.fromString("akka.tcp://ClusterSystem@127.0.0.1:".concat(receptionistPorts(0))
  //      .concat("/system/receptionist"))
  //    , ActorPath.fromString("akka.tcp://ClusterSystem@127.0.0.1:".concat(receptionistPorts(1))
  //      .concat("/system/receptionist"))
  //    , ActorPath.fromString("akka.tcp://ClusterSystem@127.0.0.1:".concat(receptionistPorts(2))
  //      .concat("/system/receptionist")))

  implicit val timeout: Timeout = Timeout(FiniteDuration.apply(responseFromServerTimeOut, TimeUnit.SECONDS))
}
