#!/usr/bin/env bash

# shellcheck disable=SC2034
# unused variables are global in nature and used in testsupport.sh

set -eo pipefail

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

# shellcheck disable=SC1091
branchName=$1
cd /home/anmol/Desktop/AbfsHadoop/hadoop-tools/hadoop-azure
. dev-support/testrun-scripts/citestsupport.sh $branchName
init

resourceDir=src/test/resources/
logdir=dev-support/testlogs/
azureTestXml=azure-auth-keys.xml
azureTestXmlPath=$resourceDir$azureTestXml
processCount=8

## SECTION: TEST COMBINATION METHODS

runHNSOAuthTest()
{
  accountName=$(xmlstarlet sel -t -v '//property[name = "fs.azure.hnsTestAccountName"]/value' $azureTestXmlPath)
  PROPERTIES=("fs.azure.account.auth.type")
  VALUES=("OAuth")
  triggerRun "HNS-OAuth" "$accountName" "$runTest" $processCount "$cleanUpTestContainers"
}

runHNSSharedKeyTest()
{
  accountName=$(xmlstarlet sel -t -v '//property[name = "fs.azure.hnsTestAccountName"]/value' $azureTestXmlPath)
  PROPERTIES=("fs.azure.account.auth.type")
  VALUES=("SharedKey")
  triggerRun "HNS-SharedKey" "$accountName"  "$runTest" $processCount "$cleanUpTestContainers"
}

runNonHNSSharedKeyTest()
{
  accountName=$(xmlstarlet sel -t -v '//property[name = "fs.azure.nonHnsTestAccountName"]/value' $azureTestXmlPath)
  PROPERTIES=("fs.azure.account.auth.type")
  VALUES=("SharedKey")
  triggerRun "NonHNS-SharedKey" "$accountName" "$runTest" $processCount "$cleanUpTestContainers"
}

runAppendBlobHNSOAuthTest()
{
  accountName=$(xmlstarlet sel -t -v '//property[name = "fs.azure.hnsTestAccountName"]/value' $azureTestXmlPath)
  PROPERTIES=("fs.azure.account.auth.type" "fs.azure.test.appendblob.enabled")
  VALUES=("OAuth" "true")
  triggerRun "AppendBlob-HNS-OAuth" "$accountName" "$runTest" $processCount "$cleanUpTestContainers"
}

runTest=true
cleanUpTestContainers=false

runHNSOAuthTest
#runHNSSharedKeyTest
#runNonHNSSharedKeyTest
#runAppendBlobHNSOAuthTest ## Keep this as the last run scenario always

if [ $runTest == true ]
then
  printAggregate
fi