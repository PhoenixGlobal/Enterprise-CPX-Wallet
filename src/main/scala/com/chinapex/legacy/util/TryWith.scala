package com.chinapex.legacy.util

/**
  * Created by ning on 16-10-21.
  */
trait TryWith {
  /**
    * have return value
    * */
  def tryWith[T](doAction: => T)(inException: Exception => T, inFinally: => Unit = {}): T = {
    try {
      doAction
    } catch {
      case e: Exception => inException(e)
    } finally {
      inFinally
    }
  }


  /**
    * no return value
    * */
  def simpleTry(doAction: => Unit ={})(inException: Exception => Unit, inFinally: => Unit = {}){
    try {
      doAction
    }
    catch {
      case e : Exception => inException(e)
    }
    finally {
      inFinally
    }
  }
  def doWithExcNoRet(e: Exception): Unit ={
    println(e)
  }
}
