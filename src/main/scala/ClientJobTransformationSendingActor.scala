import akka.actor.{Actor, ActorRef}
import akka.cluster.client.{ClusterClient, ClusterClientSettings}
import akka.pattern.Patterns

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

import java.util.UUID.randomUUID

import spray.json._
import DefaultJsonProtocol._
import Constants._

class ClientJobTransformationSendingActor extends Actor {

  var sparkSession :SparkSession = SparkSession.builder()
    .master("local[2]")
    .getOrCreate()

  val settings : ClusterClientSettings = ClusterClientSettings(context.system)
    .withInitialContacts(initialContacts)

  val id = randomUUID().toString

  val client : ActorRef = context.system.actorOf(ClusterClient.props(settings), "demo-client".concat(id))

  var columns : Map[String,String] = null.asInstanceOf[Map[String,String]]

  //var isQuery : Boolean = true

  def receive : PartialFunction[Any, Unit] = {

    case SendString(query) =>
      processSend(query)

    case SendInt(counter) =>
      processSend (text = "PING")

    case columns : Array[(String,String)] =>
      processArrayOfTuples(columns)

    case message : JsValue =>
      processJsValueArray(message)
  }

  def processSend (text:String ) :  Unit= {

//    if(text.startsWith("SELECT"))
//      isQuery = true
//    else
//      isQuery = false

    val result = Patterns.ask(client,ClusterClient.Send("/user/clusterListener", text, localAffinity = false), timeout)

    result.onComplete {
      case Success(transformationResult) =>
        self ! transformationResult

      case Failure(t) => println("An error has occured: " + t.getMessage)
    }
  }

  def processArrayOfTuples(columns : Array[(String,String)]) : Unit = {

    columns.foreach( t =>
        System.out.println(t._1.concat(" ").concat(t._2) )
    )
    if(this.columns==null)
      this.columns = columns.toMap[String,String]
    else
      this.columns = this.columns ++ columns.toMap[String,String]
  }

  def getValueFromString (fieldDataTypeStr : String, columnDataTypeStr : String): Any = {

    import java.sql.Timestamp

    columnDataTypeStr match {
      case "StringType" => fieldDataTypeStr
      case "BinaryType" => fieldDataTypeStr
      case "BooleanType" => fieldDataTypeStr.toBoolean
      case "DateType" => getDateValue(fieldDataTypeStr)
      case "TimestampType" => Timestamp.valueOf(fieldDataTypeStr)
      case "DoubleType" => fieldDataTypeStr.toDouble
      case "FloatType" => fieldDataTypeStr.toFloat
      case "ByteType" => fieldDataTypeStr.toByte
      case "IntegerType" => fieldDataTypeStr.toInt
      case "LongType" => fieldDataTypeStr.toLong
      case "ShortType" => fieldDataTypeStr.toShort
      case _ => "NONE"
    }
  }

  import java.text.SimpleDateFormat
  import java.sql.Date

  def getDateValue(fieldDataTypeStr : String) : Date = {

    val format = new SimpleDateFormat("dd-MM-yyyy")
    val parsed = format.parse(fieldDataTypeStr)
    val sqlDate = new Date(parsed.getTime)
    sqlDate
  }

  def getDataTypeFromString (columnDataTypeStr : String): DataType = {
    columnDataTypeStr match{
      case "StringType"			         => DataTypes.StringType
      case "BinaryType"             => DataTypes.StringType
      case "BooleanType"            => DataTypes.BooleanType
      case "DateType"               => DataTypes.DateType
      case "TimestampType"          => DataTypes.TimestampType
      case "DoubleType"             => DataTypes.DoubleType
      case "FloatType"              => DataTypes.FloatType
      case "ByteType"               => DataTypes.ByteType
      case "IntegerType"            => DataTypes.IntegerType
      case "LongType"               => DataTypes.LongType
      case "ShortType"              => DataTypes.ShortType
      case _ => DataTypes.StringType
    }
  }

  def processArrayOfJsValue(jsArrRows : Array[JsValue], flag : Boolean ) : Unit = {

    if(!flag){
      processArrayOfJsValue(jsArrRows)
    }

    val arrJsObjectRows : Array[JsObject] = jsArrRows.map(t=>t.convertTo[JsObject])

    import scala.collection.mutable.ListBuffer

    val rows = new java.util.ArrayList[Row]()
    val lstStructField  = new java.util.ArrayList[StructField]()

    val arrMapRows = arrJsObjectRows.map(t=> t.fields)

    var i : Int = 0

    arrMapRows.foreach(t=>{
      var rowValues = new ListBuffer[Any]()
      var j : Int = 0
      t.foreach(e=>{
          j += 1

          val value : JsValue = Option.apply[JsValue](e._2).getOrElse("NONE".toJson)

          if(!value.prettyPrint.equals("\"NONE\"")) {
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

  def processArrayOfJsValue(rows : Array[JsValue]) : Unit = {

    rows.foreach( t => {
        System.out.println("**Row Beginning**" )
        System.out.println(t.toString)
        System.out.println("**Row End**" )
      }
    )

//    if(!isQuery)
//      return

    var lstStr = scala.collection.mutable.MutableList[String]()
    var lstCol = scala.collection.mutable.MutableList[String]()
    var setCol = scala.collection.mutable.SortedSet[String]()

    val lstRow = new java.util.ArrayList[Row]()

    val arrRow : Array[scala.collection.immutable.Map[String, JsValue]] =
      rows.map(t => t.convertTo[scala.collection.immutable.Map[String, JsValue]])

    arrRow.foreach(t=>{
        System.out.println("**Row Beginning**")

        var str = new StringBuilder()
        t.keySet.foreach(s =>{
          System.out.println(s + " " + t.get(s).mkString)
          str ++= t.get(s).mkString.concat(",")
          setCol += s
        })

        lstStr += str.substring(0,str.size-1).mkString

        System.out.println("**Row End**")
    })

    setCol.foreach(t=>lstCol += t )

    arrRow.foreach(row=>{
      var arrAny : Array[Any] = new Array(0)
      for(columnName<-lstCol){

        val columnType: String = this.columns(columnName)

        var t : String = null.asInstanceOf[String]

        if(row.get(columnName).isDefined){
          t = row.get(columnName).mkString
          arrAny = arrAny :+ getValueFromString(t, columnType)
        }else{
          if(columnType.equals("StringType"))
            arrAny = arrAny :+ null.asInstanceOf[String]
          else
            arrAny = arrAny :+ null.asInstanceOf[Any]
        }
      }
      lstRow.add(Row.apply(arrAny:_*))
    })

    var fields = Array[StructField]()

    lstCol.foreach(t=>{
      val fieldDataTypeDT: DataType = getDataTypeFromString(columns.getOrElse(t, "NONE"))
      fields =  fields :+ StructField(t, fieldDataTypeDT, nullable = true)
    })

    val struct = new StructType(fields)

    var df : DataFrame = null

    try {
      df = sparkSession.createDataFrame(lstRow, struct)
      System.out.println(df.show())
    }catch{
      case ex:Exception => ex.printStackTrace()
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

      done.foreach(t=> System.out.println("***" + t + "***"))
      return
    }catch{
      case e: Exception =>
    }

    try {
      processArrayOfJsValue(message.convertTo[Array[JsValue]], flag = true)
      return
    }catch{
      case e: Exception =>
    }
  }
}
