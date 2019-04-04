# db-data-recipes

## Instructions
+ run ```source settings.sh``` at root directory
    + settings.sh sets environmental variable
        + S3 related variables
        + Dataflows postgres conn url
        + Datapackage pipeline postgres conn url
    + sets up cli autocomplete
    + install `lib` and `datapackage_pipelines_lib`

+ if you are using docker:
    + start a dataloading container named etl

    ```
    docker run -itd --name=etl\
            -v `pwd`:/home/db-data-recipes\
            --network=host\
            -w /home/db-data-recipes\
            sptkl/docker-dataloading bash
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
    + ~~ `add_filehash_to_path` --> `True` or `False` to prepend hash to path ~~

+ `dump_to_postgis` (dataflows version): 
    + e.g. 

    ```
     dump_to_postgis(resource=nycha_policeservice,
                        db_table_name=nycha_policeservice,
                        engine='postgresql://postgres:0312@localhost:5432/postgres',
                        drcSRS='EPSG:4326',
                        srcSRS='EPSG:4326',
                        )
    ```
+ `dump_to_postgis` (pipelines version): 
    + e.g. 

    ```
      - run: lib.dump_to_postgis
        parameters: 
            resource: nycha_policeservice
            db_table_name: nycha_policeservice
    ```

    + for some reason, datapacakge pipelines would only find custom pipeline plugins if they are named following `datapackage_pipelines_<plugin>` format. 
    + also the custom processor has to be placed under the `processor` folder. 

## Cli instructions
+ `cook recipe ls all` would show you all the recipes in s3
+ `cook recipe ls <recipe>` would show you all the versions of that recipe in s3
+ `cook recipe run <recupe>` would run the `./recipe/<recipe>/build.py` file 


## Docker environment
+ ```docker pull sptkl/docker-dataloading``` to get dataloading development environment
+ ```docker pull mdillon/postgis``` to get postgres database with postgis extension enabled
+ dockerfile is maintained [here](https://github.com/SPTKL/docker-dataloading)
