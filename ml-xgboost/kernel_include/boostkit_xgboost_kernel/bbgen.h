/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021. All rights reserved.
 * Description: XGBoost Kernel Bbgen header file
 */
#ifndef XGBOOST_KERNEL_BBGEN_H
#define XGBOOST_KERNEL_BBGEN_H

#include <array>
#include <climits>
#include <memory>
#include <random>
#include <vector>
#include <algorithm>

namespace Bs {
static const double BBGEN_TOL = 1.0 / 16.0;
static const size_t KFORMULA_MAX_SIZE = 64;
static const size_t KBLOCK_SIZE = 32;
static const size_t BYTE_BIT = 8;
static const size_t KBLOCK_SIZE_BIT = KBLOCK_SIZE * BYTE_BIT;
static const double HALF = 0.5;

enum LogFunction : uint8_t { K_NOT = 0, K_AND = 1 };
using RngEngine = std::mt19937;

struct Formula {
    std::array<LogFunction, KFORMULA_MAX_SIZE> funcs {};
    size_t funcsCnt = 0;
    size_t bkCnt = 0;
};

class FormulaEvaluator {
public:
    constexpr explicit FormulaEvaluator(const Formula &fm): fm_(fm) {}
    FormulaEvaluator(const FormulaEvaluator &) = delete;
    FormulaEvaluator &operator = (const FormulaEvaluator &) = delete;
    ~FormulaEvaluator() = default;
    void Evaluate(const uint64_t &src, uint64_t &dst, size_t noblocks) const;
    const Formula &GetFormula() const;

protected:
    Formula fm_;
};

using EvalPtr = std::unique_ptr<FormulaEvaluator>;
EvalPtr CreateEvaluator(const double &prob, const double &tol);

class Buffer {
public:
    Buffer()  = default;
    explicit Buffer(size_t size);
    Buffer(const Buffer &) = delete;
    Buffer &operator = (const Buffer &) = delete;
    ~Buffer() = default;
    void * const Get() const;
    size_t GetSize() const;

private:
    std::unique_ptr<void, decltype(&std::free)> ptr_ = {nullptr, &std::free};
    std::size_t size_ = 0;
};

bool GetBit(const Buffer &ptr, size_t idx);

class BatchBernGenerator {
public:
    explicit BatchBernGenerator(EvalPtr &executor): evaluator_(std::move(executor)) {}
    BatchBernGenerator(const BatchBernGenerator &) = delete;
    BatchBernGenerator &operator = (const BatchBernGenerator &) = delete;
    ~BatchBernGenerator() = default;
    size_t GetBufferSize(size_t nobits) const;
    size_t GetBlockSize() const;
    void Generate(RngEngine &rng, uint64_t &ptr, size_t size) const;

private:
    EvalPtr evaluator_ = nullptr;
};

class BGeneratorPtr {
public:
    BGeneratorPtr()  = default;
    explicit BGeneratorPtr(std::unique_ptr<BatchBernGenerator> &gener): gen_(std::move(gener)) {}
    BGeneratorPtr(const BGeneratorPtr &) = delete;
    BGeneratorPtr &operator = (const BGeneratorPtr &) = delete;
    ~BGeneratorPtr() = default;
    inline void operator()(RngEngine &rng, const Buffer &ptr) const
    {
        return gen_->Generate(rng, *(static_cast<uint64_t *>(ptr.Get())), ptr.GetSize());
    }

    inline BatchBernGenerator *Get() const
    {
        return gen_.get();
    }

    inline std::unique_ptr<Buffer> CreateBuffer(size_t nobits) const
    {
        return std::make_unique<Buffer>(gen_->GetBufferSize(nobits));
    }

private:
    std::unique_ptr<BatchBernGenerator> gen_ = {};
};

std::unique_ptr<BGeneratorPtr> CreateGenerator(double prob, double tol);
}  // namespace Bs

#endif