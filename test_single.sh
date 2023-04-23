#!/bin/bash
cd examples/mp-ping-pong
cargo run -- --impl python/$1
retVal=$?
if [ $retVal -ne 0 ]; then
    echo "Error"
    exit $retVal
fi
cd ../../
