#!/bin/bash
sql="select * from t_zone t where \$CONDITIONS"
bash ${utils_dir}/SqoopJdbc.sh customer edev_customer T_ZONE "$sql"