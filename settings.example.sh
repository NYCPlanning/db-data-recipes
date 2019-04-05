#!/bin/bash
## environmental variables settings
export AWS_ACCESS_KEY_ID='XXXXXXXXXXXXXXX'
export AWS_SECRET_ACCESS_KEY='XXXXXXXXXXX'
export S3_ENDPOINT_URL='XXXXXXXXXXXXXXXXX'
export DPP_DB_ENGINE='XXXXXXXXXXXXXXXXXXX'
export DATAFLOWS_DB_ENGINE='XXXXXXXXXXXXX'
export BUCKET='BUCKETXXXXXXXXXXXXXXXXXXXX'

## install custom python pacakges
pip install -e lib/
pip install -e datapackage_pipelines_lib/

## settings for cli
eval "$(_COOK_COMPLETE=source cook)"
