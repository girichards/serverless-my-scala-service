package common

import scala.beans.BeanProperty

case class Response(@BeanProperty message: String, @BeanProperty request: Request)
case class SimpleResponse(@BeanProperty message: String)

