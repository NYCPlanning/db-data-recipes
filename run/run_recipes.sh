#!/bin/bash

function try()
{
    [[ $- = *e* ]]; SAVED_OPT_E=$?
    set +e
}

function throw()
{
    exit $1
}

function catch()
{
    export ex_code=$?
    (( $SAVED_OPT_E )) && set +e
    return $ex_code
}

function throwErrors()
{
    set -e
}

function ignoreErrors()
{
    set +e
}

# loop through recipes and run all recipes
for f in recipes/*;
  do 
    recipe_name=`basename "$f"`
    # cook recipe run $recipe_name
    # catch || {
    #     # now you can handle
    #     case $ex_code in
    #         $AnException)
    #             echo $recipe_name gave us errors
    #         ;;
    #         $AnotherException)
    #             echo "AnotherException was thrown"
    #         ;;
    #         *)
    #             echo "An unexpected exception was thrown"
    #             throw $ex_code # you can rethrow the "exception" causing the script to exit if not caught
    #         ;;
    #     esac
    # }

  done;

