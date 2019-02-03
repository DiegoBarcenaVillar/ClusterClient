package tfm

import tfm.common.Constants._

import java.util.UUID.randomUUID

import akka.actor.{Actor, ActorRef}
import akka.cluster.client.{ClusterClient, ClusterClientSettings}
import akka.pattern.Patterns

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

class ClientJobTransformationSendingActor extends Actor {

  val settings : ClusterClientSettings = ClusterClientSettings(context.system)
    .withInitialContacts(initialContacts)

  val id : String = randomUUID().toString

  val client : ActorRef = context.system.actorOf(ClusterClient.props(settings), "client".concat(id))

  var sparkSession :SparkSession = SparkSession.builder()
    .master("local[2]")
    .getOrCreate()

  var columns : Map[String,String] = null.asInstanceOf[Map[String,String]]

  def receive : PartialFunction[Any, Unit] = {

    case SendString(query) =>
      processSend(query)

    case message : JsValue =>
      processJsValueArray(message)

    case message : String =>
      processJsValueArray(message.parseJson)
  }
  def processSend (text:String ) :  Unit= {

    val result = Patterns.ask(client,ClusterClient.Send("/user/clusterListener", text, localAffinity = false), timeout)

    result.onComplete {
      case Success(transformationResult) =>
        self ! transformationResult

      case Failure(t) => println("An error has occured: " + t.getMessage)
    }
  }
  @throws(classOf[Exception])
  def processJsValueArray(message : JsValue): Unit = {
    System.out.println("**********************")
    System.out.println("***Message Received***")
    System.out.println("**********************")

    try{
      processArrayOfTuples(message.convertTo[Array[(String,String)]])
      return
    }catch{
      case e: Exception =>
    }

    try{
      val done = message.convertTo[String]
      System.out.println("***" + done + "***")
      return
    }catch{
      case e: Exception =>
    }

    try{
      val done : Array[String] = message.convertTo[Array[String]]
      done.foreach(t=> System.out.println("***" +  t + "***"))
      return
    }catch{
      case e: Exception =>
    }

    try {
      processArrayOfJsValue(message.convertTo[Array[JsValue]], flag = true)
    }catch{
      case e: Exception =>
    }
  }
  def processArrayOfTuples(columns : Array[(String,String)]) : Unit = {

    columns.foreach( t =>
        System.out.println(t._1.concat(" ").concat(t._2) )
    )
    if(this.columns==null.asInstanceOf[Map[String,String]])
      this.columns = columns.toMap[String,String]
    else
      this.columns = this.columns ++ columns.toMap[String,String]
  }
  def processArrayOfJsValue(jsArrRows : Array[JsValue], flag : Boolean ) : Unit = {

    import scala.collection.mutable.ListBuffer

    val rows = new java.util.ArrayList[Row]()
    val lstStructField  = new java.util.ArrayList[StructField]()

    val arrMapRows = jsArrRows.map(t=>t.convertTo[JsObject]).map(t=> t.fields)

    var i : Int = 0

    arrMapRows.foreach(t=>{
      var rowValues = new ListBuffer[Any]()
      t.foreach(e=>{

        val value : JsValue = Option.apply[JsValue](e._2).getOrElse("NONE".toJson)

        if(!value.prettyPrint.equals("\"NONE\"")){
          value match{
            case value : spray.json.JsNumber =>
              rowValues += value.convertTo[Double]
            case value : spray.json.JsString =>
              rowValues += value.convertTo[String]
            case value : spray.json.JsBoolean =>
              rowValues += value.convertTo[Boolean]
          }
        }else{
          rowValues += null.asInstanceOf[Any]
        }

        var structField : StructField = null.asInstanceOf[StructField]

        if(i==0) {
          value match{
            case value : spray.json.JsNumber =>
              structField = StructField(e._1, DataTypes.DoubleType)
            case value : spray.json.JsString =>
              structField = StructField(e._1, DataTypes.StringType)
            case value : spray.json.JsBoolean =>
              structField = StructField(e._1, DataTypes.BooleanType)
          }
          lstStructField.add(structField)
        }
      })
      val row = Row.apply(rowValues:_*)
      rows.add(row)
      i += 1
    })

    val structType : StructType = StructType.apply(lstStructField)
    val df = sparkSession.createDataFrame(rows, structType)

    System.out.println(df.show(Integer.MAX_VALUE))
  }
}
