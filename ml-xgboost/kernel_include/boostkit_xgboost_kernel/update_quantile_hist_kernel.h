/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021. All rights reserved.
 * Description: XGBoost Kernel Update_Quantile_Hist_Kernel header file
 */
#ifndef XGBOOST_KERNEL_UPDATE_QUANTILE_HIST_KERNEL_H
#define XGBOOST_KERNEL_UPDATE_QUANTILE_HIST_KERNEL_H

#include <random>
#include <xgboost/base.h>
#include <xgboost/data.h>
#include <xgboost/tree_updater.h>
#include "boostkit_xgboost_kernel/bbgen.h"

namespace xgboost {
namespace tree {
static const size_t BITS_BATCH_SIZE_DEFAULT = 1024 * 32;
static const size_t GOSS_TEST_SIZE = 4 * 1024;
struct BbgenSamplingParam {
    float subsample;
    size_t tid;
    size_t discardSize;
    size_t ibegin;
    size_t iend;
    float thresholdGradVal;
};

struct InitSamplingKernelParam {
    size_t nthread;
    int iter;
    int *lastIterSampling;
    int samplingStrategy;
    int samplingStep;
    bool enableBbgen;
    float subsample;
};

bool SaveCacheChecker(int samplingStrategy, float subsample, int iter, int samplingStep);
bool KDepthWiseLossLtdLeafChecker(float minLossRatio, const float &pLastRoundBestLoss, bst_float lossChg);
void UpdateBestSplitLossChange(float &pLastRoundBestLoss,
                               size_t sizeT,
                               const bst_float &nodesBestLossChg);
void BbgenSampling(const struct BbgenSamplingParam &bbgenParam,
                   std::vector<std::mt19937> &rnds,
                   const std::vector<GradientPair> &gpair,
                   size_t &pRowIndices,
                   std::vector<size_t> &rowOffsets);
float ThresholdGradientCal(const std::vector<GradientPair> &gpair, std::mt19937 &rnd, float subsample);
void InitSamplingKernel(const std::vector<GradientPair> &gpair,
                        const DMatrix &fmat,
                        std::vector<size_t> &rowIndices,
                        std::vector<size_t> &lastSampling,
                        struct InitSamplingKernelParam initParam);
int EffSplitDenomCal(int randomSplitDenom, int maxBin);
bool SplitSkipChecker(int effSplitDenom, int32_t i, int procGridIdx);
}  // namespace tree
}  // namespace xgboost

#endif // XGBOOST_KERNEL_UPDATE_QUANTILE_HIST_KERNEL_H
