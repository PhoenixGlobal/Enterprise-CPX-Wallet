package com.chinapex

import com.chinapex.dashboard.timertask._

/**
  *
  */
package object timertask {
  val taskManager = new TaskManager()

  def start(): Unit = {
    taskManager.addTask(DashboardTimerTask.task)

  }
}
