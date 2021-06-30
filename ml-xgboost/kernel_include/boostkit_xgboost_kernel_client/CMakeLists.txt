include(${PROJECT_SOURCE_DIR}/cmake/Utils.cmake)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

option(USE_OPENMP "Build with OpenMP support." ON)
## Force AVX
option(USE_AVX "Use AVX" ON)

include(CheckSymbolExists)
include(CheckIncludeFile)
include(CheckIncludeFileCXX)
if(USE_AVX)
    if(MSVC)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /arch:AVX")
    else()
        EXECUTE_PROCESS(COMMAND uname -a OUTPUT_VARIABLE ARCH)
        message(STATUS "Current architechture is ${ARCH}")

        if (${ARCH} MATCHES ".*aarch64.*")
            message(STATUS "Turn on ARM intrinsics")
            add_compile_options(-DUSE_ARM_INTRINSICS)
        endif()
        if (${ARCH} MATCHES ".*x86.*")
            message(STATUS "Turn on Intel intrinsics")
            add_compile_options(-mavx -DUSE_INTEL_INTRINSICS)
        endif()
    endif()
endif()

add_library(boostkit_xgboost_kernel SHARED "bbgen.cpp;rabit_intrinsics.cpp;update_quantile_hist_kernel.cpp")
target_include_directories(boostkit_xgboost_kernel PUBLIC ${PROJECT_SOURCE_DIR}/kernel_include)
target_include_directories(boostkit_xgboost_kernel PRIVATE ${PROJECT_SOURCE_DIR}/include)
target_include_directories(boostkit_xgboost_kernel PRIVATE ${PROJECT_SOURCE_DIR}/rabit/include)
target_include_directories(boostkit_xgboost_kernel PRIVATE ${PROJECT_SOURCE_DIR}/dmlc-core/include)

set_target_properties(
    boostkit_xgboost_kernel PROPERTIES
    CXX_STANDARD 14
    CXX_STANDARD_REQUIRED ON)
set_output_directory(boostkit_xgboost_kernel ${PROJECT_SOURCE_DIR}/lib)

find_package(Threads REQUIRED)
if (USE_OPENMP)
    if (APPLE)
        # Require CMake 3.16+ on Mac OSX, as previous versions of CMake had trouble locating
        # OpenMP on Mac. See https://github.com/dmlc/xgboost/pull/5146#issuecomment-568312706
        cmake_minimum_required(VERSION 3.16)
    endif (APPLE)
    find_package(OpenMP REQUIRED)
    target_link_libraries(boostkit_xgboost_kernel PRIVATE OpenMP::OpenMP_CXX)
endif(USE_OPENMP)