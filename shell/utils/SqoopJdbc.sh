#!/bin/bash
connect=${source_database}
echo "[SqoopJdbc] load data to hive_database $2, from source_database connect: "+${connect}
if [ $# -lt 4 ]; then
  echo  "The usage is :  ./SqoopJdbc.sh {username} {hivedatabase} {hivetable} {sql} {map[optional]}";
elif [ $# == 4 ]; then
    sqoop import --hive-import --connect ${connect} --username $1 --password-file /user/hive/.$1 --query "$4" -m 1 --hive-database $2 --hive-table $3 --hive-overwrite --target-dir ${hive_dir}/$2.db/$3 --outdir ${tmp_dir} --bindir ${tmp_dir} -z --direct --null-string '\\N' --input-null-non-string '\\N'  --hive-delims-replacement " "
elif [ $# == 5 ]; then
    sqoop import --hive-import --connect ${connect} --username $1 --password-file /user/hive/.$1 --query "$4" --map-column-hive $5 -m 1 --hive-database $2 --hive-table $3 --hive-overwrite --target-dir ${hive_dir}/$2.db/$3 --outdir ${tmp_dir} --bindir ${tmp_dir} -z --direct --null-string '\\N' --input-null-non-string '\\N' --hive-delims-replacement " "
fi
