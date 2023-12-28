// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.spark.ml.feature

object EncoderUtils {

  val DEFAULT_THREAD_NUM = 40

  def save2PathPar(hdfsPath: String, localPath: String, numT: Int = DEFAULT_THREAD_NUM): Unit = {}
}
