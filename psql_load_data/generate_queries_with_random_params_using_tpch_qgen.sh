#!/bin/bash
for ((i=0; i<=22; i++)); do
    ./qgen -x -o "psql.out" $i > "olapqs_psql/$i.sql"
done
