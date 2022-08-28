#!/bin/bash
sql="select * from t_order t where \$CONDITIONS"
bash ${utils_dir}/SqoopJdbc.sh trade edev_order T_ORDER "$sql"