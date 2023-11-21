#!/usr/bin/env bash
set -eo pipefail
set -aux

# Copies marc files from an FTP_SERVER located in a marcive directory and posts these files to the CM_API_ENDPOINT

mkdir -p ./marc_records

cd ./marc_records

echo get marcive/* | sftp -P $FTP_PORT -i $FTP_ID_PATH -o StrictHostKeyChecking=no $FTP_USER@$FTP_SERVER 

if [[ $CM_API_ENDPOINT =~ delete ]]; then
  find_operator=""
  operation="deleting"
else
  find_operator='!'
  operation="importing"
fi

for file in $(find ~+ -type f $find_operator -regex '.*D\.[0-9]+$' -regex '.*\.[0-9]+$'); do
  echo $operation $file
  curl -F "marc_file=@$file" $CM_API_ENDPOINT 
done
