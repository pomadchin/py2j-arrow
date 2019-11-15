package com.azavea.table

import java.nio.ByteBuffer

class IntArrayTile(val array: Array[Int], val cols: Int, val rows: Int) {
  def toArray = array.clone()

  def toBytes: Array[Byte] = {
    val pixels = new Array[Byte](array.size * 32 / 8)
    val bytebuff = ByteBuffer.wrap(pixels)
    bytebuff.asIntBuffer.put(array)
    pixels
  }

  def get(col: Int, row: Int): Int = array(row * cols + col)
}
