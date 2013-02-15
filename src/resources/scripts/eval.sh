#!/bin/bash

# a script for evaluating logs resulting from a wat analysis 
# rainer.schmidt@ait.ac.at

cmd=$1
res=part-r-00000

usage () {
   echo "Usage: $0 [alllinks | ...]"
   echo "Evaluates the result of a specified pig script"
}

if [ -z "$1"]; then
   echo "parameter required"
   usage
   exit 1
fi

# extract wat files from a file URI
if [ "$cmd" = "alllinks" ]; then
   allsources=`cat alllinks_sources/$res | wc -l`
   alllinks=`cat alllinks/$res | wc -l`
   missedlinks=`cat alllinks_ulinks/$res | wc -l`
   echo "crawled resources:  $allsources";
   echo "found links:       $alllinks"; 
   echo "links not crawled: $missedlinks";
else
   echo "unknown parameter: $1"
   usage
   exit 1
fi








