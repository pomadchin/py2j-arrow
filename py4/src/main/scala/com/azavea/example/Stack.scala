package com.azavea.example

import java.util

import scala.collection.JavaConverters._

class Stack {
  private val internalList = new util.LinkedList[String]

  def push(element: String): Unit = internalList.add(0, element)

  def pop: String = internalList.remove(0)

  def getInternalList: util.List[String] = internalList

  def pushAll(elements: util.List[String]): Unit = elements.asScala.foreach(push)
}
