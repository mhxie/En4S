#!/bin/bash

# Example: sudo ../spdk/build/examples/perf -q 128 -o 4096 -O 210000 -w randrw -M 100 -t 30 -c 0x1 -L -r 'trtype:PCIe traddr:0000:00:1e.0' | grep "Tail Latency" | tr -s ' ' | cut -d':' -f2 | cut -d' ' -f2,3

MAX_RD=240000
# MAX_RD=105000
#MAX_RD=50000
MAX_WR=80000
# MAX_WR=35000
#MAX_WR=27500
let RW_TOKENS=$MAX_RD*100/$MAX_WR
RD_WEIGHT=100
DURATION=30
TOT_PTS=0
:> IOPS.data
QD=512 # not exceeding the queue size
SKIP=20000

# Define an array with your specific values
values=(100 90 75 50)

# Iterate over the array
for RD_WEIGHT in "${values[@]}"
# while [ $RD_WEIGHT -ge 0 ]
do
    let WR_WEIGHT=100-$RD_WEIGHT
    let WEIGHTED_TOKENS=$RW_TOKENS*$WR_WEIGHT+$RD_WEIGHT*100
    let REFLEX_WTOKENS=10*$WR_WEIGHT+$RD_WEIGHT
    if [[ $2 == 'fd' ]]; then
        let MAX_IOPS=$MAX_RD+$MAX_WR
	let ACTUAL_RD=$MAX_IOPS*$RD_WEIGHT/100
	let ACTUAL_WR=$MAX_IOPS-$ACTUAL_RD
	if (( $ACTUAL_RD > $MAX_RD )); then
	    let MAX_IOPS=$MAX_IOPS-$ACTUAL_RD+$MAX_RD
        elif (( $ACTUAL_WR > $MAX_WR )); then
	    let MAX_IOPS=$MAX_IOPS-$ACTUAL_WR+$MAX_WR
	fi
	let MAX_IOPS=$MAX_IOPS/1000*1000
        let MIN_IOPS=$MAX_RD*10/$WEIGHTED_TOKENS*1000
    else
        let MAX_IOPS=$MAX_RD*10/$WEIGHTED_TOKENS*1000+$RD_WEIGHT/5*1000
        let MIN_IOPS=$MAX_RD/$REFLEX_WTOKENS/10*1000
        let MIN_IOPS=1000
    fi
    let DIFF_IOPS=($MAX_IOPS-$MIN_IOPS)/$SKIP
    echo "Benchmarking for read weight =" $RD_WEIGHT "max =" $MAX_IOPS "min =" $MIN_IOPS
    echo "Total range = " $DIFF_IOPS 
    let TOT_PTS=$TOT_PTS+$DIFF_IOPS/2
    BENCH_IOPS=$MIN_IOPS
while [ $BENCH_IOPS -le $MAX_IOPS ]
do
    if [[ $1 != 'dry-run' ]]; then
        echo $RD_WEIGHT $BENCH_IOPS >> IOPS.data
        ../spdk/build/examples/perf -q 128 -o 131072 -w randwrite -t $DURATION -c 0x1 -r 'trtype:PCIe traddr:0000:00:1e.0'
        ../spdk/build/examples/perf -q $QD -o 4096 -O $BENCH_IOPS -w randrw -M $RD_WEIGHT -t $DURATION -c 0x1 -L -r 'trtype:PCIe traddr:0000:00:1e.0' | grep "Tail Latency" | tr -s ' ' | cut -d':' -f2 | cut -d' ' -f2,3  >> IOPS.data
    fi
#        | grep -e "95.00000%" -e "Total" | tr -s ' ' | cut -c 2- | cut -d' ' -f3 >> IOPS.data
    if [[ $BENCH_IOPS == $MIN_IOPS ]]; then
        BENCH_IOPS=$SKIP
    else
        BENCH_IOPS=$((BENCH_IOPS + SKIP))
    fi
done
    # let RD_WEIGHT=$RD_WEIGHT-5
    sleep 5 # avoid the remaining effects
done
echo "This benchmark takes " $TOT_PTS "minutes"

