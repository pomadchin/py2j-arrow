package com.azavea.example

import py4j.GatewayServer

object StackEntryPoint {
  def main(args: Array[String]): Unit = {
    val gatewayServer = new GatewayServer(new StackEntryPoint)
    gatewayServer.start()
    System.out.println("Gateway Server Started")
  }
}

class StackEntryPoint(val stack: Stack = new Stack()) {
  def getStack() = stack
  stack.push("Initial Item")
}