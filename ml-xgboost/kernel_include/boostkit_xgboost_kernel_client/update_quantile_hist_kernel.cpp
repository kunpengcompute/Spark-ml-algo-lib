/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021. All rights reserved.
 * Description: XGBoost Kernel Update_Quantile_Hist_Kernel source file
 */
#include "boostkit_xgboost_kernel/update_quantile_hist_kernel.h"

namespace xgboost {
namespace tree {
bool SaveCacheChecker(int samplingStrategy, float subsample, int iter, int samplingStep)
{
    return false;
}

bool KDepthWiseLossLtdLeafChecker(float minLossRatio, const float &pLastRoundBestLoss, bst_float lossChg)
{
    return false;
}

void UpdateBestSplitLossChange(float &pLastRoundBestLoss,
                               size_t sizeT,
                               const bst_float &nodesBestLossChg)
{
}

void BbgenSampling(const struct BbgenSamplingParam &bbgenParam,
                   std::vector<std::mt19937> &rnds,
                   const std::vector<GradientPair> &gpair,
                   size_t &pRowIndices,
                   std::vector<size_t> &rowOffsets)
{
}

float ThresholdGradientCal(const std::vector<GradientPair> &gpair, std::mt19937 &rnd, float subsample)
{
    return 0.0;
}

int EffSplitDenomCal(int randomSplitDenom, int maxBin)
{
    return 0;
}

bool SplitSkipChecker(int effSplitDenom, int32_t i, int procGridIdx)
{
    return false;
}

void InitSamplingKernel(const std::vector<GradientPair> &gpair,
                        const DMatrix &fmat,
                        std::vector<size_t> &rowIndices,
                        std::vector<size_t> &lastSampling,
                        struct InitSamplingKernelParam initParam)
{
}
}  // namespace tree
}  // namespace xgboost