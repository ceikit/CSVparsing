name := "CSVparsing"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.poi" % "poi" % "3.9",
  "javax.servlet" % "javax.servlet-api" % "3.0.1",
  "com.quantifind" %% "wisp" % "0.0.5"
)



    