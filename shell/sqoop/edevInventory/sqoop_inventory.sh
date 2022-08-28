#!/bin/bash
sql="select * from t_inventory t where \$CONDITIONS"
bash ${utils_dir}/SqoopJdbc.sh inventory edev_inventory T_INVENTORY "$sql"