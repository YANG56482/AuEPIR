#include <iostream>
#include "seal/seal.h"

int main() {
    auto mods = seal::CoeffModulus::BFVDefault(4096);
    std::cout << "BFVDefault(4096) size: " << mods.size() << std::endl;
    for(size_t i=0; i<mods.size(); ++i) {
        std::cout << "Mod[" << i << "]: " << mods[i].value() << " (bits: " << std::ceil(std::log2(mods[i].value())) << ")" << std::endl;
    }
    return 0;
}
