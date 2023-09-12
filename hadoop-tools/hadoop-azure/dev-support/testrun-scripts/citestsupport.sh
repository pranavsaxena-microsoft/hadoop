#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

branchName=$1
resourceDir=src/test/resources/
accountSettingsFolderName=accountSettings
combtestfile=$resourceDir
combtestfile+=abfs-combination-test-configs.xml
logdir=dev-support/testlogs/
coverageDir=target/site/jacoco/

testresultsregex="Results:(\n|.)*?Tests run:"
accountConfigFileSuffix="_settings.xml"
testOutputLogFolder=$logdir
testlogfilename=combinationTestLogFile

fullRunStartTime=$(date +%s)
STARTTIME=$(date +%s)
ENDTIME=$(date +%s)

triggerRun()
{
  echo ' '
  combination=$1
  accountName=$2
  runTest=$3
  processcount=$4
  cleanUpTestContainers=$5
  accountConfigFile=$accountSettingsFolderName/$accountName$accountConfigFileSuffix
  rm -rf $combtestfile
  cat > $combtestfile << ENDOFFILE
<configuration>

</configuration>
ENDOFFILE
  propertiessize=${#PROPERTIES[@]}
  valuessize=${#VALUES[@]}
  if [ "$propertiessize" -ne "$valuessize" ]; then
    echo "Exiting. Number of properties and values differ for $combination"
    exit 1
  fi
  for ((i = 0; i < propertiessize; i++)); do
    key=${PROPERTIES[$i]}
    val=${VALUES[$i]}
    echo "Combination specific property setting: [ key=$key , value=$val ]"
    changeconf "$key" "$val"
  done
  formatxml "$combtestfile"
  xmlstarlet ed -P -L -s /configuration -t elem -n include -v "" $combtestfile
  xmlstarlet ed -P -L -i /configuration/include -t attr -n href -v "$accountConfigFile" $combtestfile
  xmlstarlet ed -P -L -i /configuration/include -t attr -n xmlns -v "http://www.w3.org/2001/XInclude" $combtestfile
  formatxml $combtestfile
  echo ' '
  echo "Activated [$combtestfile] - for account: $accountName for combination $combination"

  if [ "$runTest" == true ]
  then
    STARTTIME=$(date +%s)
    testlogfilename="$testOutputLogFolder/Test-Logs-$combination.txt"
    touch "$testlogfilename"
    echo "Running test for combination $combination on account $accountName [ProcessCount=$processcount]"
    echo "Result can be seen in $testlogfilename"
    mvn -T 1C -Dparallel-tests=abfs -Dscale -DtestsThreadCount="$processcount" verify >> "$testlogfilename" || true
    ENDTIME=$(date +%s)
    summary
  fi

  if [ "$cleanUpTestContainers" == true ]
  then
    mvn test -Dtest=org.apache.hadoop.fs.azurebfs.utils.CleanupTestContainers
  fi

}

summary() {
  {
    echo ""
    echo "$combination"
    echo "========================"
    pcregrep -M "$testresultsregex" "$testlogfilename"
  } >> "$aggregatedTestResult"
  printf "\n----- Test results -----\n"
  pcregrep -M "$testresultsregex" "$testlogfilename"
  secondstaken=$((ENDTIME - STARTTIME))
  mins=$((secondstaken / 60))
  secs=$((secondstaken % 60))
  printf "\nTime taken: %s mins %s secs.\n" "$mins" "$secs"
  echo "Find test logs for the combination ($combination) in: $testlogfilename"
  echo "Find consolidated test results in: $aggregatedTestResult"
  echo "------------------------"
}

checkdependencies() {
  if ! [ "$(command -v pcregrep)" ]; then
    echo "Exiting. pcregrep is required to run the script."
    exit 1
  fi
  if ! [ "$(command -v xmlstarlet)" ]; then
    echo "Exiting. xmlstarlet is required to run the script."
    exit 1
  fi
}

formatxml() {
  xmlstarlet fo -s 2 "$1" > "$1.tmp"
  mv "$1.tmp" "$1"
}

changeconf() {
  xmlstarlet ed -P -L -d "/configuration/property[name='$1']" "$combtestfile"
  xmlstarlet ed -P -L -s /configuration -t elem -n propertyTMP -v "" -s /configuration/propertyTMP -t elem -n name -v "$1" -r /configuration/propertyTMP -v property "$combtestfile"
  if ! xmlstarlet ed -P -L -s "/configuration/property[name='$1']" -t elem -n value -v "$2" "$combtestfile"
  then
    echo "Exiting. Changing config property failed."
    exit 1
  fi
}

init() {
  checkdependencies
  if ! mvn clean install -DskipTests
  then
    echo ""
    echo "Exiting. Build failed."
    exit 1
  fi
  starttime=$(date +"%Y-%m-%d_%H-%M-%S")
  testOutputLogFolder+=$starttime
  mkdir -p "$testOutputLogFolder"
  aggregatedTestResult="$testOutputLogFolder/Test-Results.txt"
 }

 printAggregate() {
   echo  :::: AGGREGATED TEST RESULT ::::
   cat "$aggregatedTestResult"
  fullRunEndTime=$(date +%s)
  fullRunTimeInSecs=$((fullRunEndTime - fullRunStartTime))
  mins=$((fullRunTimeInSecs / 60))
  secs=$((fullRunTimeInSecs % 60))
  printf "\nTime taken: %s mins %s secs.\n" "$mins" "$secs"

  #/bin/bash /home/anmol/Desktop/AbfsHadoop/hadoop-tools/hadoop-azure/dev-support/testrun-scripts/codecoverage.sh

  failure_count=$(grep -oP 'Failures:\s*\K\d+' $aggregatedTestResult | awk '{ SUM += $1} END { print SUM }')
  error_count=$(grep -oP 'Errors:\s*\K\d+' $aggregatedTestResult | awk '{ SUM += $1} END { print SUM }')
  sum_failures=`expr failure_count + $error_count`
  aggregated_result=result_$(uuidgen).txt
  perl -pe 's/\x1b\[[0-9;]*[mG]//g' $aggregatedTestResult > $aggregated_result
  #cat $coverage_report >> $aggregated_result
  zipped_file=runresult_$(uuidgen).zip
  zip -r -j $zipped_file $testOutputLogFolder/*
  if [ $sum_failures -gt 0 ]
  then
      mutt -s "ABFS Hadoop Driver v2 CI Test Results, Brach: $branchName, Failures: $sum_failures" < $aggregated_result -a $zipped_file -- abfsdriver@microsoft.com
  else
      mutt -s "ABFS Hadoop Driver v2 CI Test Results, Branch: $branchName, No Failures" < $aggregated_result -a $zipped_file -- abfsdriver@microsoft.com
  fi
 }