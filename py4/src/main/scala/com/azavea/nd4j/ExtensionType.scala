package com.azavea.nd4j

import org.apache.arrow.flatbuf.Tensor
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ExtensionTypeVector
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.{FixedSizeBinaryVector, IntVector, VarBinaryVector}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.arrow.vector.types.pojo._
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType
import org.nd4j.arrow.ArrowSerde
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j

import java.nio.ByteBuffer;

class Nd4jIdentityType extends ExtensionType {

  override def storageType: ArrowType = new ArrowType.Int(32, false)

  override def extensionName: String = "nd4j-identity"

  override def extensionEquals(other: ExtensionType) = other.isInstanceOf[Nd4jIdentityType]

  override def deserialize(otherStorageType: ArrowType, serializedData: String): ArrowType = {
    if (!otherStorageType.equals(storageType)) {
      throw new UnsupportedOperationException(s"Cannot construct Nd4jType!  Received type was $otherStorageType, expected $storageType");
    }
    new Nd4jIdentityType
  }

  override def serialize(): String = ""

  override def getNewVector(name: String, fieldType: FieldType, allocator: BufferAllocator): FieldVector = {
    new Nd4jIdentityVector(name, allocator, new IntVector(name, allocator));
  }

}

class Nd4jIdentityVector(name: String, allocator: BufferAllocator, underlyingVector: IntVector)
extends ExtensionTypeVector[IntVector](name, allocator, underlyingVector) {

  def set(index: Int, arrayDim: Int) = {
    getUnderlyingVector().setSafe(index, arrayDim)
  }

  override def getObject(index: Int): Object = {
    Nd4j.eye(getUnderlyingVector.get(index))
  }

  override def hashCode(index: Int): Int = underlyingVector.hashCode(index)

}

class Nd4jType extends ExtensionType {

  override def storageType: ArrowType = new ArrowType.Binary

  override def extensionName: String = "nd4j"

  override def extensionEquals(other: ExtensionType) = other.isInstanceOf[Nd4jType]

  override def deserialize(otherStorageType: ArrowType, serializedData: String): ArrowType = {
    if (!otherStorageType.equals(storageType)) {
      throw new UnsupportedOperationException("Cannot construct Nd4jType from underlying type " + storageType);
    }
    new Nd4jType
  }

  override def serialize(): String = ""

  override def getNewVector(name: String, fieldType: FieldType, allocator: BufferAllocator): FieldVector = {
    new Nd4jVector(name, allocator, new VarBinaryVector(name, allocator));
  }

}

class Nd4jVector(name: String, allocator: BufferAllocator, underlyingVector: VarBinaryVector)
extends ExtensionTypeVector[VarBinaryVector](name, allocator, underlyingVector) {

  def set(index: Int, matr: INDArray) = {
    val tensBytes = ArrowSerde.toTensor(matr).getByteBuffer
    val len = tensBytes.position
    tensBytes.rewind
    val buf = allocator.buffer(tensBytes.limit)

    println(s"Created buffer $buf\nStarted from buffer with limit ${tensBytes.limit}, length $len, and current position ${tensBytes.position}")

    buf.setBytes(0, tensBytes)

    val holder = new NullableVarBinaryHolder
    holder.isSet = 1
    holder.start = 0
    holder.buffer = buf
    holder.end = tensBytes.limit

    getUnderlyingVector().setSafe(index, tensBytes.array)
  }

  override def getObject(index: Int): Object = {
    val tens = Tensor.getRootAsTensor(ByteBuffer.wrap(underlyingVector.getObject(index)))
    ArrowSerde.fromTensor(tens)
  }

  override def hashCode(index: Int): Int = underlyingVector.hashCode(index)
}
