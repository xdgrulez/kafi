#!/bin/bash
export KAFI_HOME=".."
if [ -z $1 ]
then
    coverage run -m unittest test_cross.Test
else
    coverage run -m unittest test_cross.Test.$1
fi
coverage html
