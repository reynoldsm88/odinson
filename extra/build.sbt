name := "odinson-extra"

libraryDependencies ++= {

  val procVersion = "7.4.4"

  Seq(
    "org.clulab" %% "processors-main" % procVersion,
    "org.clulab" %% "processors-modelsmain" % procVersion,
  )

}