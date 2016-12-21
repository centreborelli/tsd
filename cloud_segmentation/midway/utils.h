#ifndef _UTILITIES_UTILS_H
#define _UTILITIES_UTILS_H

//BBB#include "mt19937ar.h"
//BBB
//BBBinline float GenerateGaussianNoise(
//BBB        const float& sigma
//BBB        )
//BBB{
//BBB    if(!sigma) return 0.f;
//BBB    const double a = mt_genrand_res53();
//BBB    const double b = mt_genrand_res53();
//BBB    return sigma * (float) (sqrtl(-2.0l * log(a)) * cos(2.0l * M_PI * b));
//BBB}
//BBB
inline float Crop(
        const float& value,
        const float& minimum,
        const float& maximum
        )
{
    return (value > maximum) ? maximum : (value < minimum) ? minimum : value;
}

#endif //_UTILITIES_UTILS_H
