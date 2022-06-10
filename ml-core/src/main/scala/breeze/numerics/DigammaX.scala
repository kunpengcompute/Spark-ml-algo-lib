// scalastyle:off header.matches
/*
 Copyright 2012 David Hall

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package breeze.numerics

import breeze.generic.{MappingUFunc, UFunc}

import org.apache.spark.mllib.clustering.GammaX

/**
 * The derivative of the log gamma function
 */
object DigammaX extends UFunc with MappingUFunc {
  implicit object DigammaXImplInt extends Impl[Int, Double] {
    def apply(v: Int): Double = DigammaXImplDouble(v.toDouble)
  }
  implicit object DigammaXImplDouble extends Impl[Double, Double] {
    def apply(v: Double): Double = GammaX.digamma(v)
  }
}


