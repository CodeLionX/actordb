#!/usr/bin/env bash

# prints the help information using `cat` and a HEREDOC
function printHelp() {
cat <<HELP
This script generates data files in CSV format for load testing.
Usage: $0 <folder>
       [-h]
       [-m]

    -h prints this help page
    -m prints a markdown table to stdout

Example:
> $0 dataset -m
HELP
}

# reset getopts counter
OPTIND=1 # POSIX!

markdown=false
while getopts "h?m" option; do
  case "${option}" in
    m) markdown=true ;;
    h|\?) printHelp
        exit 1 ;;
  esac
done

# reset getopts counter again
shift $((OPTIND - 1))

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

  # shellcheck disable=SC2034
  r_dactors=$(ls "${folder}" | grep "${dactorName}" | wc -l)
  first_folder=$(ls "${folder}" | grep "${dactorName}" | head -1)
  # shellcheck disable=SC2034
  r_relations=$(ls "${folder}/${first_folder}" | grep ".csv" | wc -l)
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
size=$(du -sh "${folder}" | grep -oP "\d*,?\d*\w+(?=\s+.*)")

i=0
names=( )
dactors=( )
per_dactor_relations=( )
relations=( )
for name in ${dactorNames}; do
    declare n_dactors
    declare n_relations_per_dactor
    dactorDetails "${name}" n_dactors n_relations_per_dactor

    names[${i}]=${name}
    dactors[${i}]=${n_dactors}
    per_dactor_relations[${i}]=${n_relations_per_dactor}
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


if [ "${markdown}" = true ]; then
    echo "================================================================================"
    echo "Github Markdown Table"
    echo "================================================================================"
    echo ""

    echo "|  | #dactors | #relations / dactor | #relations | disk size |"
    echo "|:-|---------:|--------------------:|-----------:|----------:|"

    j=0
    while [ ${j} -lt ${i} ]; do
        echo "| ${names[${j}]} | ${dactors[${j}]} | ${per_dactor_relations[${j}]} | ${relations[${j}]} |  |"

        (( ++j ))
    done

    echo "| **Summary** | ${dactor_sum} |  | ${relations_sum} | ${size} |"
fi
