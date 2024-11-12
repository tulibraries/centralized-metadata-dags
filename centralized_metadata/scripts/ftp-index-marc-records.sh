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
error_log=error.log
fail=no

echo "These are the files that are going to get uploaded"
for file in $(find ~+ -type f $find_operator -regex '.*D\.\(mrc\|[0-9]+\)$' -regex '.*\.\(mrc\|[0-9]+\)$'); do
  echo $file
done

for file in $(find ~+ -type f $find_operator -regex '.*D\.\(mrc\|[0-9]+\)$' -regex '.*\.\(mrc\|[0-9]+\)$'); do
  touch $output_file
  touch $headers_file
  touch $error_log

  curl -s -F "marc_file=@$file" $CM_API_ENDPOINT -D $headers_file --output $output_file --no-fail 2> $error_log

  cat $output_file
  cat $error_log
  grep -i 'X-CM' $headers_file || true

  if grep -q "HTTP.*500" $headers_file; then fail=yes; fi
  sleep 1
done

if [ $fail == 'yes' ]; then exit 1; fi
