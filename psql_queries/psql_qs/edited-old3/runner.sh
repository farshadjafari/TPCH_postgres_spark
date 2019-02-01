#!/bin/bash
for filename in *.sql; do
    psql -f "$filename"
done
