/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021. All rights reserved.
 * Description: XGBoost Kernel Rabit_Intrinsics header file
 */

#ifndef BOOSTKIT_XGBOOST_KERNEL_RABIT_INTRINSICS_H
#define BOOSTKIT_XGBOOST_KERNEL_RABIT_INTRINSICS_H

#include <rabit/internal/engine.h>

namespace rabit {
namespace op {
void ReducerSumDouble(const double &srcRef, double &dstRef, int len, const MPI::Datatype &dtype);

void ReducerSumFloat(const float &srcRef, float &dstRef, int len, const MPI::Datatype &dtype);
}
}
#endif // BOOSTKIT_XGBOOST_KERNEL_RABIT_INTRINSICS_H
