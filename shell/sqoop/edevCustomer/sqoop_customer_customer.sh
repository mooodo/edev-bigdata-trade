#!/bin/bash
sql="select * from t_customer t where \$CONDITIONS"
bash ${utils_dir}/SqoopJdbc.sh customer edev_customer T_CUSTOMER "$sql"