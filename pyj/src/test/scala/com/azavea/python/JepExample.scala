package com.azavea.python

import jep._
import java.nio.ByteBuffer
import java.nio.FloatBuffer

import com.azavea.table.{IntArrayTile, IntArrowTile}
import org.scalatest._
import spire.syntax.cfor.cfor

class JepExample
  extends FunSpec
    with Matchers
    with BeforeAndAfterAll {

  lazy val jep = new SubInterpreter(new JepConfig().addSharedModules("numpy"))
  override def afterAll(): Unit = jep.close()

  describe("JepExample") {
    // https://github.com/ninia/jep/issues/28
    ignore("should fail to start and close two jep contexts sequentially with numpy modules import and without shared module") {
      intercept[JepException] {
        val jep1 = new SubInterpreter(new JepConfig())
        try {
          jep1.eval("import numpy")
        } finally {
          jep1.close()
        }
        val jep2 = new SubInterpreter(new JepConfig())
        try {
          jep2.eval("import numpy")
        } finally {
          jep2.close()
        }
      }
    }

    // https://github.com/ninia/jep/issues/28
    it("should start and close two jep contexts sequentially with numpy modules import and without shared module") {
      val jep1 = new SubInterpreter(new JepConfig().addSharedModules("numpy"))
      try {
        jep1.eval("import numpy")
      } finally {
        jep1.close()
      }
      val jep2 = new SubInterpreter(new JepConfig().addSharedModules("numpy"))
      try {
        jep2.eval("import numpy")
      } finally {
        jep2.close()
      }
    }

    it("python add(a, b) example") {
      jep.runScript("src/test/resources/python/add.py")
      val a = 2.asInstanceOf[java.lang.Integer] // invoke operates with Object types
      val b = 3.asInstanceOf[java.lang.Integer]
      // There are multiple ways to evaluate. Let us demonstrate them:
      jep.eval(s"c = add($a, $b)")
      val ans = jep.getValue("c").asInstanceOf[Long]
      println(ans)
      val ans2 = jep.invoke("add", a, b).asInstanceOf[Long]
      println(ans2)

      ans shouldBe 5
      ans2 shouldBe 5
    }

    it("nd arrays access example") {
      val data: FloatBuffer = ByteBuffer.allocateDirect(6 * 4).asFloatBuffer
      val nd: DirectNDArray[FloatBuffer] = new DirectNDArray[FloatBuffer](data, 6)
      jep.eval("import numpy")
      jep.set("x", nd)
      jep.eval("xn = numpy.array(x)")
      jep.eval("x[1] = 700")
      // val will 700 since we set it in python
      val value: Float = data.get(1)
      data.put(4, value + 100)
      // prints 800 since we set in java
      jep.eval("print(x[4])")
      jep.eval("x4 = x[4]")
      jep.eval("numpy.copyto(x, xn)") // wipe nd (!) we can control allocated array
      jep.eval("print(x)")
      jep.eval("xn1 = x[1]")
      val x4Value = jep.getValue("x4").asInstanceOf[Float]
      val txValue = jep.getValue("xn1").asInstanceOf[Float]
      println(x4Value)
      println(txValue)
      println(data.get(1))

      value shouldBe 700
      x4Value shouldBe 800
    }

    it("python ArrowTile example") {
      jep.runScript("src/test/resources/python/table.py")

      val tile = IntArrowTile((0 to 8).toArray, 3, 3)
      val bufferSize = tile.estimatedSize // 1 << 30 // 360 // we can use Int.MaxValue by default //  I'm wodnering how to compute it?
      val buffer = tile.toDirectArrowBuffer(bufferSize)
      val nd: DirectNDArray[ByteBuffer] = new DirectNDArray[ByteBuffer](buffer, bufferSize)

      // make it visible for interpreter
      jep.set("bytes", nd)
      jep.eval(s"result = arrow_test(bytes)")

      val result = jep.getValue("result").asInstanceOf[String]

      result shouldBe "Finished!"
    }

    ignore("keras example") {
      jep.runScript("src/test/resources/python/mnist_cnn.py")
      val score = jep.getValue("score[0]").asInstanceOf[Double]
      val accuracy = jep.getValue("score[1]").asInstanceOf[Double]
      println(s"score is $score and accuracy is $accuracy")
    }
  }
}
