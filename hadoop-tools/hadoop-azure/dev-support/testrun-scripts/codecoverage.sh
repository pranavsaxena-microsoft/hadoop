#!/usr/bin/env bash
. dev-support/testrun-scripts/citestsupport.sh
echo "ENTERED CODE COVERAGE SCRIPT"
az_storage_acc="sreebcitesthns"
az_container="code-coverage-report"
coverage_path=$coverageDir
echo $coverage_path
store_url="blob.core.windows.net"
HTTP_VERB="PUT"
request_date=$(date -Ru | sed 's/\+0000/GMT/')
storage_service_version="2019-12-12"
# HTTP request headers
x_ms_date_h="x-ms-date: $request_date"
x_ms_version_h="x-ms-version: $storage_service_version"
x_ms_blob_type_h="x-ms-blob-type: BlockBlob"
current_time=$(date +"%Y-%m-%d_%H-%M-%S")

sas_token="sp=racwdl&st=2022-09-23T12:04:25Z&se=2023-09-23T20:04:25Z&spr=https&sv=2021-06-08&sr=c&sig=P2YDm9%2FWjaXBSoB%2FzOHb3hxNH2xJMbocSOycgwqlJN4%3D"
sas_url="https://sreebcitesthns.blob.core.windows.net/code-coverage-report?sp=racwdl&st=2022-09-23T12:04:25Z&se=2023-09-23T20:04:25Z&spr=https&sv=2021-06-08&sr=c&sig=P2YDm9%2FWjaXBSoB%2FzOHb3hxNH2xJMbocSOycgwqlJN4%3D"
cd $coverage_path
coverage_report=codecoverage_$(uuidgen).txt
printf "\nCode Coverage report:\n" >> $coverage_report
for f in *; do
  if [ -d $f ]; then
    filename=./$f/jacoco/index.html
    file_len=$(wc --bytes < "${filename}")
    op_file="https://${az_storage_acc}.${store_url}/${az_container}/${current_time}/${f}/index.html?${sas_token}"
    blob_link=https://${az_storage_acc}.${store_url}/${az_container}/${current_time}/${f}/index.html
    echo "${op_file}"
    curl -i -X PUT --upload-file $filename -H "${x_ms_date_h}" -H "${x_ms_version_h}" -H "${x_ms_blob_type_h}" -H "Content-Length: ${file_len}" ${op_file}
    # shellcheck disable=SC2129
    printf "\n%s:\n" $f >> $coverage_report
    sed -nE 's/^.*<td>Total<([^>]+>){4}([^<]*)([^>]+>){4}([^<]*).*$/Instruction Coverage: \2, Branch Coverage: \4./p' $filename >> $coverage_report
    printf "\nLink for detailed coverage statistics: %s\n" $blob_link >> $coverage_report
  fi
done;
