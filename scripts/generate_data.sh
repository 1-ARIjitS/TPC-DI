#!/bin/env bash

SCALE_FACTOR=${1}

set -eu

cd ./Tools/

if [ -f ./PDGF ]; then
    mv PDGF pdgf
fi

../jdk1.8.0_202/bin/java -jar DIGen.jar -o ../data/sf${SCALE_FACTOR} -sf ${SCALE_FACTOR}
