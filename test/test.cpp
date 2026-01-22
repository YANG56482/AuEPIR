#include <iostream>
#include <cstdlib>
#include <ctime>

int main() {
    std::srand(std::time(nullptr));
    int random_value = std::rand();
    std::cout << "Test Value: " << random_value << std::endl;
    return 0;
}
