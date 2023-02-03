#!/bin/bash

python3 local_pandas_benchmark > pd_out.txt
python3 aidac_benchmark.py > aidac_out.txt
python3 opt_benchmark.py > rule_out.txt

