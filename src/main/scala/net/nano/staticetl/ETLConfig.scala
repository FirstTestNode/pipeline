package net.nano.staticetl

case class ETLConfig(
                      query: String,
                      jdbc: String,
                      queryName: String,
                      trackingColumnName: String,
                      pathData: String,
                      pathOffest: String
                    )
