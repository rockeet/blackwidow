//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_DEBUG_H_
#define SRC_DEBUG_H_

#if !defined(NDEBUG) && 0
#define Trace(M, ...) fprintf(stderr, "[Trace] (%s:%d) " M "\n", __FILE__, __LINE__, ##__VA_ARGS__)
#define Debug(M, ...) fprintf(stderr, "[Debug] (%s:%d) " M "\n", __FILE__, __LINE__, ##__VA_ARGS__)
#else
#define Trace(M, ...) {}
#define Debug(M, ...) {}
#endif  // NDEBUG

#endif  // SRC_DEBUG_H_
