line_count=`wc -l run.sh | cut -d' ' -f1 `
echo "${line_count}"
if [ ${line_count} -lt 100 ]; then
    echo ${line_count}
fi
