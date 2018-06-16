#!/usr/bin/env bash

function help {
cat <<HELP
This script generates data files in CSV format for load testing.
Usage: $0
       [-h]
       [-n <customers>]
       [-m <items>]
       [-k <carts>]
       -o <output_folder>

    -h prints this help page
    -n sets the total number of customers (default=100)
    -m sets the total number of items (default=100000)
    -k sets the number of carts per customer (default=20)
    -o

Example:
> mkdir output
> $0 -k 10 -o output
HELP
}

# variables with default values for switch handling
n=100
m=100000
k=20
unset outputFolder

# reset getopts counter
OPTIND=1 # POSIX!

while getopts "hn:m:k:o:" option; do
  case "${option}" in
    n) n=$OPTARG ;;
    m) m=$OPTARG ;;
    k) k=$OPTARG ;;
    o) outputFolder=$OPTARG ;;
    h|\?) help
        exit 1 ;;
  esac
done

# reset getopts again
shift $(($OPTIND - 1))

# require output folder argument and check if it is a directory
if [ -z "${outputFolder}" ]; then
  echo "You must specify an output directory!" >&2

  help
  exit 1
else
  if [ ! -d "${outputFolder}" ]; then
    echo "${outputFolder} is not a directory!"

    help
    exit 1
  fi
fi

# user info about to be generated stuff
customers=$(( n ))
c_storeVisits=$(( k * 4 ))
groups=$(( n / 20 )) # bash only supports integer division!
g_discounts=$(( m / 20 ))
carts=$(( k * n ))
c_purchases=$(( (groups * g_discounts) / carts ))
storeSections=$(( m / 200 ))
s_inventory=$(( 200 ))
s_purchases=$(( 4 * (carts * c_purchases) / storeSections ))

echo "Generating data in CSV format for load testing:"
echo "  ${customers} customers"
echo "  ${m} items"
echo "  $(( carts * c_purchases )) purchases in ${carts} carts"
echo ""
echo "Distributing data equaly to dactors:"
echo "  #customers = ${customers}"
echo "    #storeVisits = ${c_storeVisits}"
echo "  #groups = ${groups}"
echo "    #discounts = ${g_discounts}"
echo "  #carts = ${carts}"
echo "    #cart_purchases = ${c_purchases}"
echo "  #storeSections = ${storeSections}"
echo "    #inventory = ${s_inventory}"
echo "    #purchase_history = ${s_purchases}"
echo ""
echo "Output folder: ${outputFolder}"

function naming_scheme {
  local dactor=$1
  local id=$2
  local relation=$3
  declare -n ret=$4
  ret="${dactor}-${id}/${relation}.csv"
}

declare result
naming_scheme "Dactor" 12 "relation" result
echo ${result}