package com.example

import configs.Config

object FlightAnalyzer extends Config {

  def main(args: Array[String]): Unit = {
    
    args match {
      case items if (items.length == 10 || items.length == 0) => Config(args)
      case _ => throw new IllegalArgumentException(s"Not enough files, have to be 10 but ${args.length} found")
    }
    
  }  
}
