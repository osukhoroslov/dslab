#pragma once
#include "rust/cxx.h"

uint64_t benders(
        rust::Slice<const uint64_t> arrival,
        rust::Slice<const uint64_t> duration,
        rust::Slice<const uint64_t> app,
        rust::Slice<const uint64_t> app_coldstart,
        rust::Slice<const rust::Vec<uint64_t>> app_resources,
        rust::Slice<const rust::Vec<uint64_t>> host_resources,
        uint64_t keepalive,
        uint64_t iterations,
        uint64_t max_cuts);
