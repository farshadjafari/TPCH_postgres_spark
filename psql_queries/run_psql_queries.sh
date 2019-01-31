#!/bin/bash
for filename in /psql_edited/*.sql; do
    psql -i "$filename"
done
