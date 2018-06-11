package de.up.hpi.informationsystems.sampleapp

import de.up.hpi.informationsystems.sampleapp.dactors.SystemInitializer
import de.up.hpi.informationsystems.sampleapp.dactors.SystemInitializer.Startup

object Main extends App {

  println("Starting system")
  SystemInitializer.initializer ! Startup

}