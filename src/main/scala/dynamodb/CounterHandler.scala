package dynamodb


import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.spec.{GetItemSpec, UpdateItemSpec}
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, GetItemRequest, ReturnValue}
import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import common.{ApiGatewayResponse, Request, Response}

import scala.collection.JavaConverters

class CounterHandler extends RequestHandler[Request, Response] {

  val counterId = "CounterId"
  val counterValue = "CounterValue"

  def handleRequest(input: Request, context: Context): Response = {

    val logger = context.getLogger()


    logger.log("init get table name")
    val tableName = Option(System.getenv("counterTable")).getOrElse("DUMMYTABLE")

    logger.log(s"init dynamo $tableName")
    val client = AmazonDynamoDBClientBuilder.defaultClient()
    val db = new DynamoDB(client)
    val table = db.getTable(tableName)

    val get = new GetItemSpec().withPrimaryKey(counterId, "flibble").withAttributesToGet(counterValue)

    Option(table.getItem(get)) match {
      case Some(s) => Response(s"Go Serverless v1.0! Your counter function executed successfully!  Counter was ${s.getBigInteger(counterValue)}", input)
      case None => Response(s"Go Serverless v1.0! Your counter function executed successfully!  Could not find counter", input)
    }

  }
}


class IncCounterHandler extends RequestHandler[Request, Response] {

  val counterId = "CounterId"
  val counterValue = "CounterValue"

  def handleRequest(input: Request, context: Context): Response = {

    val logger = context.getLogger()


    logger.log("init get table name")
    val tableName = Option(System.getenv("counterTable")).getOrElse("DUMMYTABLE")

    logger.log(s"init dynamo $tableName")
    val client = AmazonDynamoDBClientBuilder.defaultClient()
    val db = new DynamoDB(client)
    val table = db.getTable(tableName)

    val update = new UpdateItemSpec()
      .withPrimaryKey(counterId, "flibble")
      .withUpdateExpression(s"set $counterValue = $counterValue + :val")
      .withValueMap(new ValueMap().withNumber(":val", 1))
      .withReturnValues(ReturnValue.UPDATED_NEW)

    val outcome = table.updateItem(update)
    logger.log(s"${outcome.getItem.toJSONPretty()}")

    val dbVal = outcome.getItem.get(counterValue).asInstanceOf[Number]

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
