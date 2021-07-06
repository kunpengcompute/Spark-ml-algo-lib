/*
* Copyright (C) 2021. Huawei Technologies Co., Ltd.
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
* */
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

package org.apache.spark.ml

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.Dataset


/**
 * :: DeveloperApi ::
 * Base trait for events related to MLListener
 */
@DeveloperApi
sealed trait MLListenEvent extends SparkListenerEvent

/**
 * Listener interface for Spark ML events.
 */
trait MLListener {
  def onEvent(event: MLListenEvent): Unit = {
  }
}

@DeveloperApi
case class CreatePipelineEvent(uid: String, dataset: Dataset[_]) extends MLListenEvent

@DeveloperApi
case class CreateModelEvent(uid: String) extends MLListenEvent

@DeveloperApi
case class SavePipelineEvent(uid: String, directory: String) extends MLListenEvent

@DeveloperApi
case class SaveModelEvent(uid: String, directory: String) extends MLListenEvent

@DeveloperApi
case class TransformEvent(uid: String) extends MLListenEvent

@DeveloperApi
case class LoadModelEvent(directory: String, uid: String) extends MLListenEvent
