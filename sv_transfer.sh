#! /bin/bash

export ORACLE_SID=wla
export ORACLE_HOME=/u01/app/oracle/product/12.1.0/dbhome_1
export LD_LIBRARY_PATH=$ORACLE_HOME/lib:$LD_LIBRARY_PATH

/home/oracle/anaconda3/bin/python3 /sv_scripts/log_transfer.py
