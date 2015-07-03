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
  def groups(c: String) = Action.async {
    kafkaManager.getGroupList(c).map { errorOrCMGroupsView =>
      Ok(views.html.group.groupList(c,errorOrCMGroupsView))
    }
  }
}
