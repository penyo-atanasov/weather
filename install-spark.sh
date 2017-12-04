#!/bin/bash
parent="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $parent

source spark_version
rm -r $spark_dir $spark_tar
wget $spark_url/$spark_ver/$spark_tar
tar -xzvf $spark_tar
rm $spark_tar

export SPARK_HOME=$parent/$spark_dir