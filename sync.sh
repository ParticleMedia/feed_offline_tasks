#!/bin/bash

src_file=$1
dest_dir=$2

for dir in `ls .`; do
    if [ -d $dir ]; then
        cp -f ${src_file} ${dir}/${dest_dir}
    fi
done

