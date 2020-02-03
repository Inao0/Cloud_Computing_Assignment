name := "Cloud_Computing_Assignment"

version := "0.1"

scalaVersion := "2.12.10"

// spark library dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0-preview2",
  "org.apache.spark" %% "spark-sql" % "3.0.0-preview2"
)