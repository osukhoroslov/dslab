#pragma once

#include <cstdint>

class CustomRandom {
public:
    CustomRandom(uint64_t seed) : seed_(seed) {
    }

    uint64_t Next() {
        seed_ = (kA * seed_ + kB) % kMod;
        return seed_;
    }

private:
    uint64_t seed_;

    static inline constexpr uint64_t kA = 8213228977, kB = 1969342019, kMod = 1e9 + 7;
};
