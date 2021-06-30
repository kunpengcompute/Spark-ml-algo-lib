/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021. All rights reserved.
 * Description: XGBoost Kernel Bbgen source file
 */
#include "boostkit_xgboost_kernel/bbgen.h"

namespace Bs {
void FormulaEvaluator::Evaluate(const uint64_t &src, uint64_t &dst, size_t noblocks) const
{
}

const Formula &FormulaEvaluator::GetFormula() const
{
    return fm_;
}

EvalPtr CreateEvaluator(const double &prob, const double &tol)
{
    return nullptr;
}

Buffer::Buffer(size_t size)
{
}

void * const Buffer::Get() const
{
    return nullptr;
}

size_t Buffer::GetSize() const
{
    return 0;
}

bool GetBit(const Buffer &ptr, size_t idx)
{
    return false;
}

size_t BatchBernGenerator::GetBlockSize() const
{
    return 0;
}

size_t BatchBernGenerator::GetBufferSize(size_t bitsCnt) const
{
    return 0;
}

void BatchBernGenerator::Generate(RngEngine &rng, uint64_t &ptr, size_t size) const
{
}

std::unique_ptr<BGeneratorPtr> CreateGenerator(double prob, double tol)
{
    return nullptr;
}
} // namespace Bs
