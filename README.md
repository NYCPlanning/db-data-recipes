# db-data-recipes [![CircleCI](https://circleci.com/gh/NYCPlanning/db-data-recipes.svg?style=svg)](https://circleci.com/gh/NYCPlanning/db-data-recipes)

## Instructions
+ run ```source settings.sh``` at root directory
    + settings.sh sets environmental variable
        + S3 related variables
        + Dataflows postgres conn url
        + Datapackage pipeline postgres conn url
    + sets up cli autocomplete
    + install lib 

+ if you are using docker:
    + start a dataloading container named etl

    ```
    docker run -itd --name=etl\
            -v `pwd`:/home/db-data-recipes\
            --network=host\
            -w /home/db-data-recipes\
            sptkl/docker-dataloading /bin/bash -c "sh settings.sh; bash"
    ```
    or 

    ```
    docker run -itd --name=etl\
            -v `pwd`:/home/db-data-recipes\
            --network=host\
            -w /home/db-data-recipes\
            sptkl/docker-dataloading /bin/bash -c "pip install -e .; bash"
    ```
    + Within the etl docker container, download the beautifulsoup package for web scraping
    ```
    pip install bs4
    ```

    + start a postgis container named db

    ```
    docker run -itd --name=db\
            --network=host\
            mdillon/postgis
    ```

    + then do ```docker exec -it etl bash``` to access interactive bash

+ About dump_to_s3
  + adapted from datapackage-piplines-aws
  + set environmental variables as below:

    ```
    AWS_ACCESS_KEY_ID='XXXXXXXXXXX'
    AWS_SECRET_ACCESS_KEY='XXXXXXXXXXXXXXX'
    S3_ENDPOINT_URL='https://XXX.XXXXXXX.com'
    ```

  + `params` is a dictionary that covers:
    + `bucket` --> name of the bucket
    + `acl` --> access control
    + `path` --> file path
    + `add_filehash_to_path` --> `True` or `False` to prepend hash to path

+ `dump_to_postgis` (dataflows version): 
    + e.g. 

    ```
     dump_to_postgis(resource='nycha_policeservice',
                        db_table_name='nycha_policeservice',
                        engine='postgresql://postgres:0312@localhost:5432/postgres',
                        drcSRS='EPSG:4326',
                        srcSRS='EPSG:4326',
                        )
    ```
    + by default if db_table_name is not specified, we will use resource name as the db_table name. 

## Cli instructions
+ `cook recipe ls s3` would show you all the recipes in s3
+ `cook recipe ls <recipe>` would show you all the versions of that recipe in s3
+ `cook recipe run <recupe>` would run the `./recipe/<recipe>/build.py` file 


## Docker environment
+ ```docker pull sptkl/docker-dataloading``` to get dataloading development environment
+ ```docker pull mdillon/postgis``` to get postgres database with postgis extension enabled
+ dockerfile is maintained [here](https://github.com/SPTKL/docker-dataloading)

## Archive
+ Phasing out datapacakge-pipelines 
    + main reason is because developing custom plugins are difficult
    + two different code base to maintain, inconsistant
    + dataflows is designed to replace pipelines

## About version control
+ __seperation of putting and pulling:__ 
    + putting: pull from open data and load into s3
        + versioned by dates
        + md5 checksum on files to avoid duplication 
        + note that datapackage.json contains the hash code for the csv files, hence if the csv changes, we would update both csv and datapacakge.json because both files would have a different hash. In another way of saying, for the csv/json pair files, _it is impossible to update one but not the other._
    + pulling: pull from s3 and load into production database
+ __using datapackage like a datapackage__: 
    + for example when we build facdb, we will have one versioned directory with all the inputing csv and one datapacakge.json file that has detailed info about alll the csvs. 
    + we only have to load the datapacakge.json for facdb and then do a one time dump_to_postgis, which would load everything to postgis. 
