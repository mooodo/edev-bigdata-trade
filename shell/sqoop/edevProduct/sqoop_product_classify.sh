#!/bin/bash
sql="select * from t_classify t where \$CONDITIONS"
bash ${utils_dir}/SqoopJdbc.sh product edev_product T_CLASSIFY "$sql"