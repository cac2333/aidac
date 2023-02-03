#!/bin/bash

table=$1
path=$2

psql -c "\copy $table TO '$path/$table.csv' delimiter ',' csv"

psql -d sf04 --user=sf04 -c "\copy lineitem TO '/home/db/xwang223/aidac/datasets/sf04/lineitem.csv' delimiter ',' csv HEADER"