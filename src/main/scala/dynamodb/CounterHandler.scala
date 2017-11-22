package dynamodb


import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import common.{ApiGatewayResponse, Request, Response}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, GetItemRequest}

import scala.collection.JavaConverters


trait DDB {

  val tableName = "HelloCounter"
  val client = AmazonDynamoDBClientBuilder.defaultClient()
  val keyMap = Map("counterId" -> new AttributeValue().withS("flibble"))
  val keyMapJava = JavaConverters.mapAsJavaMap(keyMap)
  val req = new GetItemRequest().withTableName(tableName).withAttributesToGet("counterValue").withKey(keyMapJava)

}


class CounterHandler extends RequestHandler[Request, Response] {

  def handleRequest(input: Request, context: Context): Response = {

    val logger = context.getLogger()


    logger.log("init get table name")
    val tableName = Option(System.getenv("counterTable")).getOrElse("DUMMYTABLE")

    logger.log("init dynamo")
    val client = AmazonDynamoDBClientBuilder.defaultClient()
    val keyMap = Map("counterId" -> new AttributeValue().withS("flibble"))
    val keyMapJava = JavaConverters.mapAsJavaMap(keyMap)
    val req = new GetItemRequest().withTableName(tableName).withAttributesToGet("counterValue").withKey(keyMapJava)

    val result = client.getItem(req)

    val item = result.getItem()


    val dbVal = if (item == null) {
      logger.log(s"item was null")
      "was null"
    } else if (item.isEmpty) {
      logger.log("item was empty")
      "was empty"
    } else {
      JavaConverters.mapAsScalaMap(item).get("counterValue").map(_.getN).getOrElse("no value")
    }

    Response(s"Go Serverless v1.0! Your counter function executed successfully!  dbVal was $dbVal", input)
  }
}


class ApiGatewayCounterHandler extends RequestHandler[Request, ApiGatewayResponse] {

  def handleRequest(input: Request, context: Context): ApiGatewayResponse = {
    val headers = Map("x-custom-response-header" -> "my custom response header value")
    ApiGatewayResponse(200, "Go Serverless v1.0! Your counter function executed successfully via APIGatewayHandler!",
      JavaConverters.mapAsJavaMap[String, Object](headers),
      true)
  }
}
