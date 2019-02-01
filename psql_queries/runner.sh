#!/bin/bash
for filename in *.sql; do
    echo "$filename"
    psql -f "$filename"
done
