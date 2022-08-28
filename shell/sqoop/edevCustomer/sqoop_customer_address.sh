#!/bin/bash
sql="select * from t_address t where \$CONDITIONS"
bash ${utils_dir}/SqoopJdbc.sh customer edev_customer T_ADDRESS "$sql"