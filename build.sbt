ThisBuild / scalaVersion := "2.12.17"

lazy val DataCenterPrices = project
    .in(file("."))
    .settings(
        name := "DataCenterPrices",
        libraryDependencies ++= Seq(
            "org.apache.spark" %% "spark-core" % "3.5.0",
            "org.apache.spark" %% "spark-mllib" % "3.5.0" % "provided",
            "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided"
        )
    )
