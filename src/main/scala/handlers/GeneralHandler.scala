package handlers

import com.amazonaws.services.lambda.runtime.events.SNSEvent
import com.amazonaws.services.lambda.runtime.{Context, RequestHandler}
import common.SimpleResponse

import scala.collection.JavaConverters._

class GeneralHandler extends RequestHandler[SNSEvent, SimpleResponse] {

  def handleRequest(input: SNSEvent, context: Context): SimpleResponse = {

    val logger = context.getLogger()

    input.getRecords().asScala.foreach {

      r =>
        logger.log(
          s"""
             |Message
             |=======
             | Source:          ${r.getEventSource}
             | SubscriptionArn: ${r.getEventSubscriptionArn}
             | Event Version:   ${r.getEventVersion}
             | Message:         ${r.getSNS.getMessage}
             | Subject:         ${r.getSNS.getSubject}
             | MessageId:       ${r.getSNS.getMessageId}
             | Topic ARN:       ${r.getSNS.getTopicArn}
             | MessageType:     ${r.getSNS.getType}
             |
       """.stripMargin)


    }
    
    SimpleResponse("Go Serverless v1.0! Your function executed successfully!")
  }

}
