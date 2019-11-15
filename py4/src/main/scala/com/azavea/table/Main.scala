package com.azavea.table

import spire.syntax.cfor._

object Main {
  def main(args: Array[String]): Unit = {
    val tile = new IntArrayTile((0 to 8).toArray, 3, 3)
    val atile = IntArrowTile((0 to 8).toArray, 3, 3)

    // test that our structures match
    cfor(0)(_ < 3, _ + 1) { c =>
      cfor(0)(_ < 3, _ + 1) { r =>
        if(tile.get(c, r) != atile.get(c, r))
          throw new Exception("Oops your {Array/Arrow}Tiles don't work")
      }
    }

    val natile = IntArrowTile.fromArrowBytes(atile.toArrowBytes, 3, 3)
    cfor(0)(_ < 3, _ + 1) { c =>
      cfor(0)(_ < 3, _ + 1) { r =>
        if(natile.get(c, r) != atile.get(c, r))
          throw new Exception("Oops your {Array/Arrow}Tiles don't work")
      }
    }


    println("Hello World!")
  }
}
