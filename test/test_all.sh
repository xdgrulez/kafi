#!/bin/bash
export KAFI_HOME=".."
#
coverage run -a -m unittest test_kafka_cluster.Test
coverage run -a -m unittest test_kafka_restproxy.Test
coverage run -a -m unittest test_fs_azureblob.Test
coverage run -a -m unittest test_fs_local.Test
coverage run -a -m unittest test_fs_s3.Test
#
coverage run -a -m unittest test_cross_storage.Test
#
coverage html
