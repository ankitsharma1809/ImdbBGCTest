resolvers ++= Seq(Resolver.url("bintray-spark-packages", url("https://dl.bintray.com/spark-packages/maven/")),
Resolver.url("bintray-sbt-plugins", url("http://dl.bintray.com/sbt/sbt-plugin-releases/")))
addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "5.2.4")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.5")