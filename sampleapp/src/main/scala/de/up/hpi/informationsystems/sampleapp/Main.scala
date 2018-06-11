package de.up.hpi.informationsystems.sampleapp

import de.up.hpi.informationsystems.sampleapp.dactors.SystemInitializer

object Main extends App {

  println("Starting system")
  SystemInitializer.initializer ! SystemInitializer.Startup

  import sun.misc.Signal
  Signal.handle(new Signal("INT"), signal => {
    println(s"received $signal")
    SystemInitializer.initializer ! SystemInitializer.Shutdown
  })

}