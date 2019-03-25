# db-data-recipes

## Instructions
+ run ```pip install --editable .``` at root directory to install lib
    + lib is a set of custom tools we built for dataflows and datapackage pipelines
+ run ```pip install -r requirements.txt``` for the rest of dependencies
+ if you are using docker:
    ```
    docker run --rm -it\
        -v `pwd`:/home/db-data-recipes\
        -w /home/db-data-recipes\
        sptkl/docker-dataloading bash
    ```
    run at root directory

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


## Docker environment
+ ```docker pull sptkl/docker-dataloading``` to get development environment
+ dockerfile is maintained [here](https://github.com/SPTKL/docker-dataloading)
