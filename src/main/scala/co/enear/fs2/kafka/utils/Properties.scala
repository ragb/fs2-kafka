package co.enear.fs2.kafka.util

import java.util.{ Properties => JavaProperties }

object Properties {

  def fromMap(map: Map[String, String]) = map.foldLeft(new JavaProperties) {
    case (ac, (key, value)) =>
      ac.setProperty(key, value)
      ac
  }

}
