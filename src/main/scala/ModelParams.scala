import helpers.EventID

object ModelParams {
  val eventNames: List[EventID] = List("order", "cart", "wish", "click")

  val indicatorParamsList = Some(
    List(
      IndicatorParams(
        "order", Some(3000), Some(300)),
      IndicatorParams(
        "cart", Some(3000), Some(300)),
      IndicatorParams(
        "wish", Some(3000), Some(300)),
      IndicatorParams(
        "click", Some(3000), Some(300))
    )
  )

  val indicatorParamsList2 = eventNames.map(
    eventName => IndicatorParams(
      eventName, Some(3000), Some(300))
  )
}

object DefaultModelParams {
  val MaxEventsPerEventType = 500
  val MaxCorrelatorsPerEventType = 300
  val MaxQueryEvents = 100
}


case class IndicatorParams(
                            name: EventID,
                            MaxEventsPerEventType: Option[Int],
                            MaxCorrelatorsPerEventType: Option[Int]
                          ) extends Serializable

object ItemSimParams {
  val w2vDim = 200
  val minCount = 0
  val neighborNum = 200
}

object ItemPopParams {
  val interval = 60
}