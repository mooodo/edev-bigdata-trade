#!/bin/bash
sql="select * from t_city t where \$CONDITIONS"
bash ${utils_dir}/SqoopJdbc.sh customer edev_customer T_CITY "$sql"