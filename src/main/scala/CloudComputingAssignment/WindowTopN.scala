package CloudComputingAssignment

case class WindowTopN(time_window: MyTimeWindow,
                      currentCount: Array[((GridCell, GridCell), Int)]
                     )
