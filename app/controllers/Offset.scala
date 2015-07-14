package controllers

import play.api.mvc.{Controller, Action}

/**
 * User: wxb
 * Date: 2015/7/3
 * DESCRIPTION: 
 */
object Offset extends Controller {
  import play.api.libs.concurrent.Execution.Implicits.defaultContext
  private[this] val kafkaManager = KafkaManagerContext.getKafkaManager
  def offsets(c: String, g: String) = Action.async {
    kafkaManager.getOffsetList(c, g).map { CMOffsetsView =>
      Ok(views.html.offset.OffsetList(c,CMOffsetsView))
    }
  }
}
