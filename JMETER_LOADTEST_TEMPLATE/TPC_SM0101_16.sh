#!/bin/bash

TEST=TPC_SM0101_16
END=.jmx
EpochTime=$(date +%s)
DateTime=$(date +%F_%H%M)
UNDER=_
OUT_NAME=$DateTime$UNDER$TEST
JMETER=JMETER
NOHUP=NOHUP

/Users/hbarile/jmeter_install_dir/apache-jmeter-5.3/bin/jmeter -n -t 01_JMX/$TEST$END -l 03_RESULTS/$OUT_NAME.csv -f -e -o 04_OUTPUT/$OUT_NAME >02_LOGS/$OUT_NAME.log&
mv nohup.out 02_LOGS/$OUT_NAME$UNDER$NOHUP.log && touch nohup.out
mv jmeter.log 02_LOGS/$OUT_NAME$UNDER$JMETER.log && touch jmeter.log