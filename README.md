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

## Docker environment
+ ```docker pull sptkl/docker-dataloading``` to get development environment
+ dockerfile is maintained [here](https://github.com/SPTKL/docker-dataloading)
