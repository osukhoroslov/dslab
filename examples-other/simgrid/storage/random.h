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

    static inline constexpr uint64_t kA = 737687, kB = 65916437, kMod = 1e9 + 7;
};
