#!/bin/bash

# This will create a set of folders that can be used to set up a new test
# If you're doing a copy and paste of the JMETER_LOADTEST folder you do not need to run this. 

FOLDER=BENCHMARK

mkdir $FOLDER
mkdir $FOLDER/00_SETUP
mkdir $FOLDER/01_JMX
mkdir $FOLDER/02_LOGS
mkdir $FOLDER/03_RESULTS
mkdir $FOLDER/04_OUTPUT
mkdir $FOLDER/05_WORKBOOKS
mkdir $FOLDER/09_TRASH
