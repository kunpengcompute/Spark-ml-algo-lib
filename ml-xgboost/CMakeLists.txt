cmake_minimum_required(VERSION 3.13)
set(CMAKE_CXX_FLAGS "-fstack-protector-all -D_FORTIFY_SOURCE=2 -O2 -Wl,-z,relro,-z,now,-z,noexecstack -s ${CMAKE_CXX_FLAGS}")
set(CMAKE_SKIP_RPATH TRUE)
project(xgboost LANGUAGES CXX C VERSION 1.1.0)
include(cmake/Utils.cmake)
list(APPEND CMAKE_MODULE_PATH "${xgboost_SOURCE_DIR}/cmake/modules")
cmake_policy(SET CMP0022 NEW)
cmake_policy(SET CMP0079 NEW)
cmake_policy(SET CMP0063 NEW)

if ((${CMAKE_VERSION} VERSION_GREATER 3.13) OR (${CMAKE_VERSION} VERSION_EQUAL 3.13))
  cmake_policy(SET CMP0077 NEW)
endif ((${CMAKE_VERSION} VERSION_GREATER 3.13) OR (${CMAKE_VERSION} VERSION_EQUAL 3.13))

message(STATUS "CMake version ${CMAKE_VERSION}")

if (CMAKE_COMPILER_IS_GNUCC AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5.0)
  message(FATAL_ERROR "GCC version must be at least 5.0!")
endif()

include(${xgboost_SOURCE_DIR}/cmake/FindPrefetchIntrinsics.cmake)
find_prefetch_intrinsics()
include(${xgboost_SOURCE_DIR}/cmake/Version.cmake)
write_version()
set_default_configuration_release()

#-- Options
option(BUILD_C_DOC "Build documentation for C APIs using Doxygen." OFF)
option(USE_OPENMP "Build with OpenMP support." ON)
option(BUILD_STATIC_LIB "Build static library" OFF)
## Bindings
option(JVM_BINDINGS "Build JVM bindings" ON)
option(R_LIB "Build shared library for R package" OFF)
## Dev
option(USE_DEBUG_OUTPUT "Dump internal training results like gradients and predictions to stdout.
Should only be used for debugging." OFF)
option(GOOGLE_TEST "Build google tests" OFF)
option(WITH_CODE_COVERAGE "with code coverage" OFF)
option(USE_DMLC_GTEST "Use google tests bundled with dmlc-core submodule" OFF)
option(USE_NVTX "Build with cuda pwrofiling annotations. Developers only." OFF)
set(NVTX_HEADER_DIR "" CACHE PATH "Path to the stand-alone nvtx header")
option(RABIT_MOCK "Build rabit with mock" OFF)
option(HIDE_CXX_SYMBOLS "Build shared library and hide all C++ symbols" OFF)
## CUDA
option(USE_CUDA  "Build with GPU acceleration" OFF)
option(USE_NCCL  "Build with NCCL to enable distributed GPU support." OFF)
option(BUILD_WITH_SHARED_NCCL "Build with shared NCCL library." OFF)
set(GPU_COMPUTE_VER "" CACHE STRING
  "Semicolon separated list of compute versions to be built against, e.g. '35;61'")
## Copied From dmlc
option(USE_HDFS "Build with HDFS support" OFF)
option(USE_AZURE "Build with AZURE support" OFF)
option(USE_S3 "Build with S3 support" OFF)
## Sanitizers
option(USE_SANITIZER "Use santizer flags" OFF)
option(SANITIZER_PATH "Path to sanitizes.")
set(ENABLED_SANITIZERS "address" "leak" CACHE STRING
  "Semicolon separated list of sanitizer names. E.g 'address;leak'. Supported sanitizers are
address, leak, undefined and thread.")
## Plugins
option(PLUGIN_LZ4 "Build lz4 plugin" OFF)
option(PLUGIN_DENSE_PARSER "Build dense parser plugin" OFF)
## Force AVX
option(USE_AVX "Use AVX" ON) 

#-- Checks for building XGBoost
if (USE_DEBUG_OUTPUT AND (NOT (CMAKE_BUILD_TYPE MATCHES Debug)))
  message(SEND_ERROR "Do not enable `USE_DEBUG_OUTPUT' with release build.")
endif (USE_DEBUG_OUTPUT AND (NOT (CMAKE_BUILD_TYPE MATCHES Debug)))
if (USE_NCCL AND NOT (USE_CUDA))
  message(SEND_ERROR "`USE_NCCL` must be enabled with `USE_CUDA` flag.")
endif (USE_NCCL AND NOT (USE_CUDA))
if (BUILD_WITH_SHARED_NCCL AND (NOT USE_NCCL))
  message(SEND_ERROR "Build XGBoost with -DUSE_NCCL=ON to enable BUILD_WITH_SHARED_NCCL.")
endif (BUILD_WITH_SHARED_NCCL AND (NOT USE_NCCL))
if (JVM_BINDINGS AND R_LIB)
  message(SEND_ERROR "`R_LIB' is not compatible with `JVM_BINDINGS' as they both have customized configurations.")
endif (JVM_BINDINGS AND R_LIB)
if (R_LIB AND GOOGLE_TEST)
  message(WARNING "Some C++ unittests will fail with `R_LIB` enabled,
 as R package redirects some functions to R runtime implementation.")
endif (R_LIB AND GOOGLE_TEST)

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

if(WITH_CODE_COVERAGE)
    message(STATUS "Enabled code coverage options")
    add_compile_options("-ftest-coverage" "-fprofile-arcs")
    # actually these options are for compiling but for some targets they are needed in link options
    add_link_options("-ftest-coverage" "-fprofile-arcs")
    add_link_options("-lgcov")
endif()

#-- Sanitizer
if (USE_SANITIZER)
  include(cmake/Sanitizer.cmake)
  enable_sanitizers("${ENABLED_SANITIZERS}")
endif (USE_SANITIZER)

if (USE_CUDA)
  SET(USE_OPENMP ON CACHE BOOL "CUDA requires OpenMP" FORCE)
  # `export CXX=' is ignored by CMake CUDA.
  set(CMAKE_CUDA_HOST_COMPILER ${CMAKE_CXX_COMPILER})
  message(STATUS "Configured CUDA host compiler: ${CMAKE_CUDA_HOST_COMPILER}")

  enable_language(CUDA)
  set(GEN_CODE "")
  format_gencode_flags("${GPU_COMPUTE_VER}" GEN_CODE)
  message(STATUS "CUDA GEN_CODE: ${GEN_CODE}")
endif (USE_CUDA)

find_package(Threads REQUIRED)

if (USE_OPENMP)
  if (APPLE)
    # Require CMake 3.16+ on Mac OSX, as previous versions of CMake had trouble locating
    # OpenMP on Mac. See https://github.com/dmlc/xgboost/pull/5146#issuecomment-568312706
    cmake_minimum_required(VERSION 3.16)
  endif (APPLE)
  find_package(OpenMP REQUIRED)
endif (USE_OPENMP)

# dmlc-core
msvc_use_static_runtime()
add_subdirectory(${xgboost_SOURCE_DIR}/dmlc-core)
set_target_properties(dmlc PROPERTIES
  CXX_STANDARD 14
  CXX_STANDARD_REQUIRED ON
  POSITION_INDEPENDENT_CODE ON)
list(APPEND LINKED_LIBRARIES_PRIVATE dmlc)

# rabit
set(RABIT_BUILD_DMLC OFF)
set(DMLC_ROOT ${xgboost_SOURCE_DIR}/dmlc-core)
set(RABIT_WITH_R_LIB ${R_LIB})
add_subdirectory(rabit)

if (RABIT_MOCK)
  list(APPEND LINKED_LIBRARIES_PRIVATE rabit_mock_static)
else()
  list(APPEND LINKED_LIBRARIES_PRIVATE rabit)
endif(RABIT_MOCK)
foreach(lib rabit rabit_base rabit_empty rabit_mock rabit_mock_static)
  # Explicitly link dmlc to rabit, so that configured header (build_config.h)
  # from dmlc is correctly applied to rabit.
  if (TARGET ${lib})
    target_link_libraries(${lib} dmlc ${CMAKE_THREAD_LIBS_INIT})
    if (HIDE_CXX_SYMBOLS)  # Hide all C++ symbols from Rabit
      set_target_properties(${lib} PROPERTIES CXX_VISIBILITY_PRESET hidden)
    endif (HIDE_CXX_SYMBOLS)
  endif (TARGET ${lib})
endforeach()

# Exports some R specific definitions and objects
if (R_LIB)
  add_subdirectory(${xgboost_SOURCE_DIR}/R-package)
endif (R_LIB)

# core xgboost
list(APPEND LINKED_LIBRARIES_PRIVATE Threads::Threads ${CMAKE_THREAD_LIBS_INIT})
add_subdirectory(${xgboost_SOURCE_DIR}/plugin)
add_subdirectory(${xgboost_SOURCE_DIR}/src)
target_link_libraries(objxgboost PUBLIC dmlc)
set(XGBOOST_OBJ_SOURCES "${XGBOOST_OBJ_SOURCES};$<TARGET_OBJECTS:objxgboost>")

#-- library
if (BUILD_STATIC_LIB)
  add_library(xgboost STATIC ${XGBOOST_OBJ_SOURCES})
  string(TIMESTAMP time_stamp "%s")
  add_custom_command(TARGET xgboost
          POST_BUILD
          COMMAND mkdir ${CMAKE_CURRENT_SOURCE_DIR}/lib/tmp_${time_stamp}
          COMMAND mv ${CMAKE_CURRENT_SOURCE_DIR}/lib/libxgboost.a ${CMAKE_CURRENT_SOURCE_DIR}/lib/tmp_${time_stamp}/
          COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/lib/libboostkit_xgboost_kernel.a ${CMAKE_CURRENT_SOURCE_DIR}/lib/tmp_${time_stamp}/
          VERBATIM
          )
  add_custom_command(TARGET xgboost
          POST_BUILD
          COMMAND ar -x libxgboost.a
          COMMAND ar -x libboostkit_xgboost_kernel.a
          COMMAND sh -c "ar -qcs ${CMAKE_CURRENT_SOURCE_DIR}/lib/libxgboost.a ${CMAKE_CURRENT_SOURCE_DIR}/lib/tmp_${time_stamp}/*.o"
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/lib/tmp_${time_stamp}/
          VERBATIM
          )
  add_custom_command(TARGET xgboost
          POST_BUILD
          COMMAND rm -rf ${CMAKE_CURRENT_SOURCE_DIR}/lib/tmp_${time_stamp}
          VERBATIM
          )
else (BUILD_STATIC_LIB)
  add_library(xgboost SHARED ${XGBOOST_OBJ_SOURCES})
  target_link_libraries(xgboost PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/lib/libboostkit_xgboost_kernel.so)
endif (BUILD_STATIC_LIB)

#-- Hide all C++ symbols
if (HIDE_CXX_SYMBOLS)
  set_target_properties(objxgboost PROPERTIES CXX_VISIBILITY_PRESET hidden)
  set_target_properties(xgboost PROPERTIES CXX_VISIBILITY_PRESET hidden)
endif (HIDE_CXX_SYMBOLS)

target_include_directories(xgboost
  INTERFACE
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_PREFIX}/include>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_LIST_DIR}/include>)
target_link_libraries(xgboost PRIVATE ${LINKED_LIBRARIES_PRIVATE})

# This creates its own shared library `xgboost4j'.
if (JVM_BINDINGS)
  # To ensure open source independently compliation
  add_subdirectory(${PROJECT_SOURCE_DIR}/kernel_include/boostkit_xgboost_kernel_client)
  add_subdirectory(${xgboost_SOURCE_DIR}/jvm-packages)
endif (JVM_BINDINGS)
#-- End shared library

#-- CLI for xgboost
add_executable(runxgboost ${xgboost_SOURCE_DIR}/src/cli_main.cc ${XGBOOST_OBJ_SOURCES})

target_include_directories(runxgboost
  PRIVATE
  ${xgboost_SOURCE_DIR}/include
  ${xgboost_SOURCE_DIR}/dmlc-core/include
  ${xgboost_SOURCE_DIR}/rabit/include
  ${xgboost_SOURCE_DIR}/kernel_include)
target_link_libraries(runxgboost PRIVATE ${LINKED_LIBRARIES_PRIVATE})
target_link_libraries(runxgboost PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/lib/libboostkit_xgboost_kernel.so)
set_target_properties(
  runxgboost PROPERTIES
  OUTPUT_NAME xgboost
  CXX_STANDARD 14
  CXX_STANDARD_REQUIRED ON)
#-- End CLI for xgboost

set_output_directory(runxgboost ${xgboost_SOURCE_DIR})
set_output_directory(xgboost ${xgboost_SOURCE_DIR}/lib)
# Ensure these two targets do not build simultaneously, as they produce outputs with conflicting names
add_dependencies(xgboost runxgboost)

#-- Installing XGBoost
if (R_LIB)
  set_target_properties(xgboost PROPERTIES PREFIX "")
  if (APPLE)
    set_target_properties(xgboost PROPERTIES SUFFIX ".so")
  endif (APPLE)
  setup_rpackage_install_target(xgboost ${CMAKE_CURRENT_BINARY_DIR})
  set(CMAKE_INSTALL_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/dummy_inst")
endif (R_LIB)
if (MINGW)
  set_target_properties(xgboost PROPERTIES PREFIX "")
endif (MINGW)

if (BUILD_C_DOC)
  include(cmake/Doc.cmake)
  run_doxygen()
endif (BUILD_C_DOC)

include(GNUInstallDirs)
# Install all headers.  Please note that currently the C++ headers does not form an "API".
install(DIRECTORY ${xgboost_SOURCE_DIR}/include/xgboost
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(TARGETS xgboost runxgboost
  EXPORT XGBoostTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  INCLUDES DESTINATION ${LIBLEGACY_INCLUDE_DIRS})
install(EXPORT XGBoostTargets
  FILE XGBoostTargets.cmake
  NAMESPACE xgboost::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/xgboost)

include(CMakePackageConfigHelpers)
configure_package_config_file(
  ${CMAKE_CURRENT_LIST_DIR}/cmake/xgboost-config.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/cmake/xgboost-config.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/xgboost)
write_basic_package_version_file(
  ${CMAKE_BINARY_DIR}/cmake/xgboost-config-version.cmake
  VERSION ${XGBOOST_VERSION}
  COMPATIBILITY AnyNewerVersion)
install(
  FILES
  ${CMAKE_BINARY_DIR}/cmake/xgboost-config.cmake
  ${CMAKE_BINARY_DIR}/cmake/xgboost-config-version.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/xgboost)


# For MSVC: Call msvc_use_static_runtime() once again to completely
# replace /MD with /MT. See https://github.com/dmlc/xgboost/issues/4462
# for issues caused by mixing of /MD and /MT flags
msvc_use_static_runtime()
