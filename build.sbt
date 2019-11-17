name := "BGC-test"
version := "1.0"
scalaVersion := "2.11.0"
val sparkVersion = "2.2.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % sparkVersion % "provided"
libraryDependencies += "com.github.scopt" % "scopt_2.11" % "4.0.0-RC2"


assemblyJarName in assembly := "BGC-test-1.0.jar"