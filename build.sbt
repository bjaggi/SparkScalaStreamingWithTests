val sparkVersion = "1.6.1"
name := "ScalaPrograms"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-twitter" % sparkVersion,
  
  "org.scalactic" %% "scalactic" % "3.0.4",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "junit" % "junit" % "4.12",
  "com.holdenkarau" %% "spark-testing-base" % "1.6.1_0.3.2"


)