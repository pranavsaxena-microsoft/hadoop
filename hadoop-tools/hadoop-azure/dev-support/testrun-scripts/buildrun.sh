#!/usr/bin/env sh

branchName=$1
cd /home/anmol/Desktop/AbfsHadoop
git config credential.helper store
git checkout $branchName
git pull origin $branchName
echo "CD-ing into the hadoop-azure dir"
cd /home/anmol/Desktop/AbfsHadoop/hadoop-tools/hadoop-azure
filebuild=build_$(uuidgen).txt
echo "filebuild : $filebuild"
mvn clean install -DskipTests >> $filebuild
echo "Here"
filerun=runresult_$(uuidgen).txt
perl -pe 's/\x1b\[[0-9;]*[mG]//g' $filebuild > $filerun
if grep -q "BUILD FAILURE" $filerun;
then
  mutt -s "ABFS Hadoop Driver CI Test Results: Build Failure" < $filerun -- abfsdriver@microsoft.com
else
  cd /home/anmol/Desktop/AbfsHadoop/hadoop-tools/hadoop-azure
  ./dev-support/testrun-scripts/runcitests.sh $branchName
fi
rm -rf $filebuild
rm -rf $filerun