package com.azavea.nd4j

import py4j.GatewayServer
import org.apache.arrow.flatbuf.Tensor
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowFileReader, ArrowFileWriter, ArrowStreamReader}
import org.apache.arrow.vector.types.pojo._
import org.nd4j.arrow.ArrowSerde
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.Collections

object Nd4jEntryPoint {
  def main(args: Array[String]): Unit = {
    ExtensionTypeRegistry.register(new Nd4jIdentityType)
    val gatewayServer = new GatewayServer(new Nd4jEntryPoint)
    gatewayServer.start()
    System.out.println("Gateway Server Started")
  }
}

class Nd4jEntryPoint {
  def identity(n: Int) = {
    val tens = ArrowSerde.toTensor(Nd4j.eye(n))
    tens.getByteBuffer.array
  }

  def send(bytes: Array[Byte]) = {
    println(s"Received bytes of size ${bytes.size}")
    val bb = ByteBuffer.wrap(bytes.slice(1, bytes.size))
    val tens = Tensor.getRootAsTensor(bb)
    val matr = ArrowSerde.fromTensor(tens)
    println(matr)
  }

  def send_batch(bytes: Array[Byte]) = {
    println(s"Received bytes of size ${bytes.size}")
    val stream = new java.io.ByteArrayInputStream(bytes)
    val root = new RootAllocator
    val asr = new ArrowStreamReader(stream, root)
    asr.loadNextBatch
    val schema = asr.getVectorSchemaRoot
    val vec = schema.getVector("eyes")
    val n = schema.getRowCount
    val matrs = Range(0, n).map{ i => vec.getObject(i).asInstanceOf[INDArray] }
    println(s"Content was $n identity matrices with dimension ${matrs.map{ m => (m.rows, m.columns) }}")
  }
}



object ExtensionTypeTest {
  def main(args: Array[String]): Unit = {
    ExtensionTypeRegistry.register(new Nd4jType)
    val schema = new Schema(Collections.singletonList(Field.nullable("a", new Nd4jType)))
    val allocator = new RootAllocator(Int.MaxValue)
    val root = VectorSchemaRoot.create(schema, allocator)

    val matr = Nd4j.eye(40)
    val vector = root.getVector("a").asInstanceOf[Nd4jVector]
    vector.setValueCount(1)
    vector.set(0, matr)
    root.setRowCount(1)

    val file = File.createTempFile("nd4jtest", ".arrow")
    val channel = FileChannel.open(Paths.get(file.getAbsolutePath()), StandardOpenOption.WRITE)
    val writer = new ArrowFileWriter(root, null, channel)
    writer.start
    writer.writeBatch
    writer.end
  }
}

class ExtensionTypeEntryPoint
