#!/usr/bin/env bash

while read v; do
    cd "data_100_mb/${v}"
    echo "Entering directory $PWD"
    find -type d -not -name "." -exec mv "{}" "../{}00099" ";"
    cd "../.."
done < $1