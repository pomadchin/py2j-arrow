package com.azavea.table

import py4j.GatewayServer

object TableEntryPoint {
  def main(args: Array[String]): Unit = {
    val gatewayServer = new GatewayServer(new TableEntryPoint)
    gatewayServer.start()
    System.out.println("Gateway Server Started")
  }
}

class TableEntryPoint(val tile: IntArrowTile = IntArrowTile((0 to 8).toArray, 3, 3)) {
  def getTile() = tile
}