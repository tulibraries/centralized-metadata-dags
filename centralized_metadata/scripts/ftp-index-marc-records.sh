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

output_file=output.json
headers_file=headers_file.txt
fail=no

for file in $(find ~+ -type f $find_operator -regex '.*D\.[0-9]+$' -regex '.*\.[0-9]+$'); do
  curl -s -F "marc_file=@$file" $CM_API_ENDPOINT -D $headers_file --output $output_file --no-fail
  cat $output_file
  grep -i 'X-CM' $headers_file

  if [ grep -q "500" $headers_file ]; then  fail=yes; fi
done

if [ $fail == 'yes' ]; then exit 1; fi
