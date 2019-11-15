package com.azavea.table


import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types.pojo.{Field, Schema}
import org.apache.arrow.vector.ipc.ArrowStreamReader

import java.io._

import scala.collection.JavaConverters._

class IntArrowTile(val array: IntVector, val cols: Int, val rows: Int) {
  import IntArrowTile._

  def schema =
    new VectorSchemaRoot(
      List(fieldArr).asJava,
      List(array.asInstanceOf[FieldVector]).asJava,
      cols * rows
    )

  def toBytes: Array[Byte] = {
    // WARN: contains no header information
    val buf = array.getDataBuffer.nioBuffer()
    val arr = new Array[Byte](buf.remaining)
    buf.get(arr)
    arr
  }

  def toArrowBytes: Array[Byte] = {
    val buffer = new ByteArrayOutputStream
    val writer = new ArrowStreamWriter(schema, null, buffer)
    writer.start()
    // TODO: paging! at this point everything is a single batch
    writer.writeBatch
    writer.end()
    buffer.toByteArray
  }

  def get(col: Int, row: Int): Int = array.get(row * cols + col)
}

// TODO: use flat buffers instead of POJO (Plain Old Java Objects)?
object IntArrowTile {
  val fieldArr = new Field("array",
    new FieldType(true, new ArrowType.Int(32, true), null, null),
    Nil.asJava
  )

  // TODO: can we ecnode cols/rows and still perform paging? look into ND4J
  val fieldCols = new Field("cols",
    new FieldType(true, new ArrowType.Int(32, true), null, null),
    Nil.asJava
  )

  val fieldRows = new Field("rows",
    new FieldType(true, new ArrowType.Int(32, true), null, null),
    Nil.asJava
  )

  val schema: Schema = new Schema(List(fieldArr).asJava)

  // row * cols + col
  def apply(arr: Array[Int], cols: Int, rows: Int): IntArrowTile = {
    val allocator = new RootAllocator(Long.MaxValue)
    val root = VectorSchemaRoot.create(schema, allocator)

    // val vec = new IntVector("array", allocator)
    // root.getFieldVectors.get(0).allocateNew()
    val vec = new IntVector("array", allocator)

    vec.allocateNew(arr.length)
    arr.zipWithIndex.foreach { case (v, i) => vec.set(i, v) }
    vec.setValueCount(arr.length)
    root.setRowCount(arr.length)
    new IntArrowTile(vec, cols, rows)
  }

  def fromArrowBytes(arr: Array[Byte], cols: Int, rows: Int): IntArrowTile = {
    val allocator = new RootAllocator(1L << 30)
    val in = new ByteArrayInputStream(arr)
    val reader = new ArrowStreamReader(in, allocator)
    val readVector = reader.getVectorSchemaRoot().getFieldVectors().get(0).asInstanceOf[IntVector]

    // actually that is how paging is done
    // it is possible to iterate through batches
    val batchLoaded = reader.loadNextBatch()

    new IntArrowTile(readVector, cols, rows)
  }

}
