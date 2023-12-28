// scalastyle:off header.matches
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.ml.neighbors

import scala.reflect.ClassTag

import org.apache.spark.ml.linalg.DenseMatrix

final case class BatchData[T: ClassTag] (
    featureMatrix: DenseMatrix,
    featureNorms: Array[Double],
    auxInfos: Array[T]) extends Serializable {}
