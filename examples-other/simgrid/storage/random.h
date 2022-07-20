#pragma once

#include <cstdint>
#include <iostream>

class CustomRandom {
public:
    CustomRandom(uint64_t seed) : seed_(seed) {
        std::cerr << "Created linear random engine with parameters:" << std::endl;
        std::cerr << "A     = " << kA << std::endl;
        std::cerr << "B     = " << kB << std::endl;
        std::cerr << "MOD   = " << kMod << std::endl;
        std::cerr << "SEED  = " << seed << std::endl;
    }

    uint64_t Next() {
        seed_ = (kA * seed_ + kB) % kMod;
        return seed_;
    }

private:
    uint64_t seed_;

    static inline constexpr uint64_t kA = 737687, kB = 65916437, kMod = 1e9 + 7;
};
