#pragma once
#include "rust/cxx.h"

uint64_t lp_lower_bound(
        rust::Slice<const uint64_t> arrival,
        rust::Slice<const uint64_t> duration,
        rust::Slice<const uint64_t> app,
        rust::Slice<const uint64_t> app_coldstart,
        uint64_t keepalive,
        uint64_t init_estimate /* -1 <=> no estimate */);
