#!/usr/bin/env bash


# Copies marc files from an FTP_SERVER located in a marcive directory and posts these files to the CM_API_ENDPOINT

mkdir -p ./marc_records

cd ./marc_records


echo get marcive/* sftp -P $FTP_PORT -i $FTP_ID_PATH $FTP_USER@$FTP_SERVER 

for file in $(ls); do
  curl -F "marc_file=@$file" $CM_API_ENDPOINT 
done
