#!/usr/bin/env bash

###############################################################################
# Function declarations
###############################################################################

# prints the help information using `cat` and a HEREDOC
function printHelp() {
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
    -o output folder that must already exist before running this script

Example:
> mkdir output
> $0 -k 10 -o output
HELP
}

# opens a file handle to a dynamically created file path
# arg1: dactor name
# arg2: id of dactor
# arg3: relation name
# arg4: return variable reference
function open_file() {
  local dactor=$1
  local id=$2
  local relation=$3
  # creates a nameref pointing to the variable denoted by `$4`
  # see https://stackoverflow.com/questions/3236871/how-to-return-a-string-value-from-a-bash-function/38997681#38997681
  declare -n file=$4

  folder="${outputFolder}/${dactor}-${id}"
  mkdir -p ${folder}
  file="${folder}/${relation}.csv"
}

# pseudo-rand function using a simple counter
# arg1: min
# arg2: max
# returns result via `echo`
rand_counter=0
function rand() {
  local min=$1
  local max=$2
  local next=${rand_counter}

  (( ++rand_counter ))
  echo $(( min + (next % max) ))
}

# write store section files
function writeStoreSections() {
  echo "Writing store sections ..."

  i=0
  while [ ${i} -lt ${storeSections} ]; do

    if [ $(( i % 100)) -eq 0 ]; then
      echo "  processed ${i} sections"
    fi

    declare f_ss_inventory
    open_file "StoreSection" ${i} "inventory" f_ss_inventory
    echo "${inventoryHeader}" > "${f_ss_inventory}"

    j=0
    while [ ${j} -lt ${s_inventory} ]; do
      echo "${items[$(( (i * s_inventory) + j ))]}" >> "${f_ss_inventory}"

      (( ++j ))
    done

    declare f_ss_purchases
    open_file "StoreSection" ${i} "purchase_history" f_ss_purchases
    echo "${purchaseHistoryHeader}" > "${f_ss_purchases}"

    j=0
    maxCustomerId=$(( customers - 1 ))
    minInventoryId=$(( i * s_inventory ))
    maxInventoryId=$(( (i + 1) * s_inventory ))
    maxInventoryId=$(( maxInventoryId < m ? maxInventoryId : m ))

    while [ ${j} -lt ${s_purchases} ]; do
      customerId=$(rand 0 ${maxCustomerId})
      inventoryId=$(rand ${minInventoryId} ${maxInventoryId})
      echo "${inventoryId},${timeValue},${j},${customerId}" >> "${f_ss_purchases}"

      (( ++j ))
    done


    (( ++i ))
  done
}

# write group manager files
function writeGroupManagers() {
  echo "Writing group managers ..."

  i=0
  while [ ${i} -lt ${groups} ]; do

    if [ $(( i % 100)) -eq 0 ]; then
      echo "  processed ${i} groups"
    fi

    declare f_group_discounts
    open_file "GroupManager" ${i} "discounts" f_group_discounts
    echo "${discountsHeader}" > "${f_group_discounts}"

    j=0
    while [ ${j} -lt ${g_discounts} ]; do
      id=$(rand 0 ${m})
      disc=$(rand 0 100)
      echo "${id}, ${id}.${disc}" >> "${f_group_discounts}"

      (( ++j ))
    done

    (( ++i ))
  done
}

# write customer files
function writeCustomers() {
  echo "Writing customers ..."

  i=0
  while [ ${i} -lt ${customers} ]; do

    if [ $(( i % 100)) -eq 0 ]; then
      echo "  processed ${i} customers"
    fi

    declare f_c_customerInfo
    open_file "Customer" ${i} "customer_info" f_c_customerInfo
    echo "${customerInfoHeader}" > "${f_c_customerInfo}"
    echo "Hubert Blaine Wolfeschlegelsteinhausenbergerdorff,$(( i % groups ))" >> "${f_c_customerInfo}"

    declare f_c_passwd
    open_file "Customer" ${i} "passwd" f_c_passwd
    echo "${passwdHeader}" > "${f_c_passwd}"
    echo "lkj435432lkh532lkj45325lh43253ölk432:DSA_FSA:R$:§" >> "${f_c_passwd}"

    declare f_c_storeVisits
    open_file "Customer" ${i} "store_visits" f_c_storeVisits
    echo "${storeVisitsHeader}" > "${f_c_storeVisits}"

    j=0
    while [ ${j} -lt ${c_storeVisits} ]; do
      echo "${j},${timeValue},$(( i * j )),${i}.${j},${j}.${i}" >> "${f_c_storeVisits}"

      (( ++j ))
    done

    (( ++i ))
  done
}

# write cart files
function writeCarts() {
  echo "Writing carts ..."

  i=0
  sessionId=123987
  while [ ${i} -lt ${carts} ]; do

    if [ $(( i % 100)) -eq 0 ]; then
      echo "  processed ${i} carts"
    fi

    declare f_c_cartInfo
    open_file "Cart" ${i} "cart_info" f_c_cartInfo
    echo "${cartInfoHeader}" > "${f_c_cartInfo}"
    echo "$(( i % customers )),${i},${sessionId}" >> "${f_c_cartInfo}"

    declare f_c_cartPurchases
    open_file "Cart" ${i} "cart_purchases" f_c_cartPurchases
    echo "${cartPurchasesHeader}" > "${f_c_cartPurchases}"

    j=0
    while [ ${j} -lt ${c_purchases} ]; do
      iventoryId=$(rand 0 ${m})
      echo "$(( ((i + 1) * j) % storeSections )),${sessionId},${inventoryId},${inventoryId},${inventoryId}.${inventoryId},${inventoryId}.${inventoryId},${inventoryId}.${inventoryId}" >> "${f_c_cartPurchases}"

      (( ++j ))
    done

    (( ++i ))
  done
}


###############################################################################
# Parsing and checking input
###############################################################################

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


###############################################################################
# Init vars
###############################################################################

scriptDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# calculate all counts depending on input
customers=$(( n ))
c_storeVisits=$(( k * 4 ))
groups=$(( n / 20 + 1 )) # bash only supports integer division!
g_discounts=$(( m / 20 ))
carts=$(( k * n ))
c_purchases=$(( (groups * g_discounts) / carts ))
storeSections=$(( m / 400 + 1))
s_inventory=$(( 400 ))
s_purchases=$(( 2 * (carts * c_purchases) / storeSections ))

# user info about to be generated stuff
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
echo ""

# default time value
timeValue="2017-05-02T11:01:32Z"

# headers:
# - StoreSection
inventoryHeader="i_id,i_price,i_min_price,i_quantity,i_var_disc"
purchaseHistoryHeader="i_id,time,i_quantity,c_id"

# - GroupManager
discountsHeader="i_id,fixed_disc"

# - Customer
customerInfoHeader="cust_name,c_g_id"
storeVisitsHeader="store_id,time,amount,fixed_disc,var_disc"
passwdHeader="enc_passwd"

# - Cart
cartInfoHeader="c_id,store_id,session_id"
cartPurchasesHeader="sec_id,session_id,i_id,i_quantity,i_fixed_disc,i_min_price,i_price"


# generate inventory data using an array
items=( )

i=0
while [ ${i} -lt ${m} ]; do
  quantity=$(( i * 10 ))
  items[${i}]="${i},${i}.${i},${i}.${i},${quantity},${i}.5"

  (( ++i ))
done


###############################################################################
# Main script: Generate data and write it into files
###############################################################################

# store pids of sub processes in array
pids=( )

writeStoreSections &
pids[0]=$!

writeGroupManagers &
pids[1]=$!

writeCustomers &
pids[2]=$!

writeCarts &
pids[3]=$!

# wait for sub processes to finish and catch failure
resultCode=0
for pid in ${pids[*]}; do
  wait ${pid} || resultCode=1
done

if [ "${resultCode}" != "0" ]; then
  echo "There was an error during data generation"
  exit 1
fi

echo "Finished!"
exit 0