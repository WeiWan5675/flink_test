object Test2 {
  def main(args: Array[String]) {
    val sites = Map("runoob" -> "www.runoob.com", "google" -> "www.google.com")

    println("show(sites.get( \"runoob\")) : " +
      show(sites.get( "runoob")) )
    println("show(sites.get( \"baidu\")) : " +
      show(sites.get( "baidu")) )
  }

  def show(x: Option[String]) = x match {
    case Some(s) => s
    case None => "?"
  }
}