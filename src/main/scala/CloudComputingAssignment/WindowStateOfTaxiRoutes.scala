package CloudComputingAssignment

case class WindowStateOfTaxiRoutes(time_window: MyTimeWindow,
                                   currentCount: Map[(GridCell, GridCell), Int]
                                  )
