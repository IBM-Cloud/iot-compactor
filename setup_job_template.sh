#!/usr/bin/env bash

ibmcloud target -g default
if [ $? -ne 0 ]; then
	echo You need to be logged on to IBM Cloud with the ibmcloud CLI tool.
	echo Run \"ibmcloud login --sso\" first.
	exit 1
else
	echo Setting up IOT compactor infrastructure in above IBM Cloud account, region and resource group...
fi

set -e

echo Selecting code engine project...
ibmcloud ce project select --name iot-kafka-generator 

echo Creating code engine job with docker image torsstei/iot-compactor...
set +e; ibmcloud ce job delete --name iot-compactor-job --force &> /dev/null; set -e
ibmcloud ce job create --name iot-compactor-job   --maxexecutiontime 7200 --image docker.io/torsstei/iot-compactor:latest \
 -e APIKEY=<###### Your API Key here ######> \
 -e SQL_INSTANCE_CRN=<###### Your SQL Query instance CRN here ######> \
 -e RESULT_LOCATION=<###### Your default COS result location here ######>

echo "Creating cron based event trigger for code engine job twice per day (after midnight and early afternoon)..."
set +e; ibmcloud ce subscription cron delete --name iot-compactor-job-trigger --force &> /dev/null; set -e
ibmcloud ce subscription cron create -n iot-compactor-job-trigger -d iot-compactor-job --destination-type job  -s '30 1/12 * * *'

echo Done!

