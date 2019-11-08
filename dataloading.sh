for f in recipes/*
do
    name=$(basename $f)
    docker exec etl cook recipe run $name
done