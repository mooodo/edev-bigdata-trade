#!/bin/bash
sql="select * from t_product t where \$CONDITIONS"
bash ${utils_dir}/SqoopJdbc.sh product edev_product T_PRODUCT "$sql"