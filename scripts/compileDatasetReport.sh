#!/usr/bin/env bash

# Script arguments:
folder=$1

# require folder argument and check if it is a directory
if [ -z "${folder}" ]; then
    echo "You must specify a directory!" >&2
    exit 1
else
    if [ ! -d "${folder}" ]; then
        echo "${folder} is not a directory!"
        exit 1
    fi
fi

# check directory contents to adhere to our generation schema
ls "${folder}"| grep ".*-.*" >/dev/null 2>&1
if ! [ $? -eq 0 ]; then
    echo "The specified directory does not contain a valid dataset!"
    exit 1
fi

function dactorDetails() {
  local dactorName=$1
  # creates a nameref pointing to the variable denoted by `$4`
  # see https://stackoverflow.com/questions/3236871/how-to-return-a-string-value-from-a-bash-function/38997681#38997681
  declare -n r_dactors=$2
  declare -n r_relations=$3

  r_dactors=$(ls "${folder}" | grep "${dactorName}" | wc -l)
  r_relations=$(ls "${folder}/${dactorName}-0" | grep ".csv" | wc -l)
}

# create report
echo ""
echo "================================================================================"
echo "Report about dataset in"
echo "${folder}"
echo "================================================================================"
echo ""

# search for dactor names
dactorNames=$(ls "${folder}" | grep -oP "\w*(?=-\d*)" | uniq)
# get disk size
size=$(du -sh "${folder}")

i=0
dactors=( )
relations=( )
for name in ${dactorNames}; do
    declare n_dactors
    declare n_relations_per_dactor
    dactorDetails "${name}" n_dactors n_relations_per_dactor

    dactors[${i}]=${n_dactors}
    relations[${i}]=$(( n_dactors * n_relations_per_dactor ))

    # write results
    echo "${name}"
    echo "----------------------------------------"
    echo "Number of dactors:    ${n_dactors}"
    echo "Relations per dactor: ${n_relations_per_dactor}"
    echo "Relations:            ${relations[${i}]}"
    echo ""

    (( ++i ))
done

# sum up array contents
read <<< "${dactors[@]/%/+}0"
dactor_sum=$(( REPLY ))

read <<< "${relations[@]/%/+}0"
relations_sum=$(( REPLY ))

echo "SUMMARY"
echo "========================================"
echo "Dactor types: ${i}"
echo "Dactors: ${dactor_sum}"
echo "Relations: ${relations_sum}"
echo ""
echo "Size on disk: ${size}"
echo ""