/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy

import java.net.URI

private[spark] case class ApplicationDescription(
    name: String,    //application名字，可以通过spark.app.name进行设置，也可以通过代码中setAppName()进行设置
    maxCores: Option[Int],  //最大Cpu Cores
    memoryPerExecutorMB: Int, //每个Executor的内存
    command: Command,   //启动命令
    appUiUrl: String, //application的 UI Url地址
    eventLogDir: Option[URI] = None, //event日志目录
    // short name of compression codec used when writing event logs, if any (e.g. lzf)
    //  编写事件日志时使用的压缩编解码器的简称，如果有配置的话(例如lzf)
    eventLogCodec: Option[String] = None,
    coresPerExecutor: Option[Int] = None,
    // number of executors this application wants to start with,
    // only used if dynamic allocation is enabled
    // 这个Application想要启动的executor数，只有在动态分配启用时使用
    initialExecutorLimit: Option[Int] = None,
    user: String = System.getProperty("user.name", "<unknown>")) {

  override def toString: String = "ApplicationDescription(" + name + ")"
}
