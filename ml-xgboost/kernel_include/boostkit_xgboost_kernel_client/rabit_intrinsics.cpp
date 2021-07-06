/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021. All rights reserved.
 * Description: XGBoost Kernel Rabit_Intrinsics source file
 */
#include "boostkit_xgboost_kernel/rabit_intrinsics.h"

#ifdef USE_INTEL_INTRINSICS
#include <immintrin.h>
#endif
#ifdef USE_ARM_INTRINSICS
#include <arm_neon.h>
#endif

namespace rabit {
namespace op {
#ifdef USE_INTEL_INTRINSICS
void ReducerSumFloat(const float &srcRef, float &dstRef, const int len, const MPI::Datatype &dtype)
{
}

void ReducerSumDouble(const double &srcRef, double &dstRef, const int len, const MPI::Datatype &dtype)
{
}
#endif

#ifdef USE_ARM_INTRINSICS
void ReducerSumDouble(const double &srcRef, double &dstRef, int len, const MPI::Datatype &dtype)
{
}

void ReducerSumFloat(const float &srcRef, float &dstRef, int len, const MPI::Datatype &dtype)
{
}
#endif
}  // namespace op
}  // namespace rabit