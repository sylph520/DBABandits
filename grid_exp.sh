#!/bin/bash

LAMBDAS=(0.01 0.1 0.5 0.8)
DELTA1S=(0.001 0.01 0.05 0.1 0.3 0.5)
DELTA2S=(0.01 0.02 0.002 0.0002 0.00002 0.000002 0.0000002)
TAUS=(3 5 8 10 15 25 50 70)

for l in "${LAMBDAS[@]}"; do
  for d1 in "${DELTA1S[@]}"; do
    for d2 in "${DELTA2S[@]}"; do
      for tau in "${TAUS[@]}"; do
        echo "Running lambda=$l, delta1=$d1, delta2=$d2, tau=$tau"
        python simulation/sim_c3ucb_vR.py --dynamic_flag --lambda $l --delta1 $d1 --delta2 $d2 --tau $tau ${@}
      done
    done
  done
done
