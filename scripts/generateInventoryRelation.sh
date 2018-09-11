#!/usr/bin/env bash

# functions
#############################
# prints the help document
function printHelp() {
cat <<HELP
This script generates a file in CSV format containing items.
Usage: $0
       [-h]
       [-n <items>]
       -o <output_folder>

    -h prints this help page
    -n sets the total number of items (default=100000)
    -o output folder that must already exist before running this script

Example:
> mkdir output
> $0 -n 10 -o output
HELP
}

# parse input
#############################
# variables with default values for switch handling
n=100000
unset outputFolder

# reset getopts counter
OPTIND=1 # POSIX!

while getopts "hn:o:" option; do
  case "${option}" in
    n) n=$OPTARG ;;
    o) outputFolder=$OPTARG ;;
    h|\?) printHelp
        exit 1 ;;
  esac
done

# reset getopts counter again
shift $(($OPTIND - 1))

# require output folder argument and check if it is a directory
if [ -z "${outputFolder}" ]; then
  echo "You must specify an output directory!" >&2

  printHelp
  exit 1
else
  if [ ! -d "${outputFolder}" ]; then
    echo "${outputFolder} is not a directory!"

    printHelp
    exit 1
  fi
fi


# constants
#############################
inventoryHeader="i_id,i_price,i_min_price,i_quantity,i_var_disc"


# main
#############################

# write inventory file
echo "Writing inventory ..."

f_ss_inventory="${outputFolder}/inventory.csv"
echo "${inventoryHeader}" > "${f_ss_inventory}"

i=0
stringBuilder=""
while [ ${i} -lt ${n} ]; do
    quantity=$(( i * 10 ))
    stringBuilder="${stringBuilder}${i},${i}.${i},${i}.${i},${quantity},${i}.5"$'\n'

    if [ $(( i % 10000)) -eq 0 ]; then
        echo "  processed ${i} items"
        echo -e -n "${stringBuilder}" >> "${f_ss_inventory}"
        stringBuilder=""
    fi

    (( ++i ))
done

echo "  processed ${i} items"
echo -e -n "${stringBuilder}" >> "${f_ss_inventory}"
stringBuilder=""

echo "... finished writing inventory."