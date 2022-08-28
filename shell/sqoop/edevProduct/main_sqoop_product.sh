#!/bin/bash
cd $(pwd)
source app.conf
bash sqoop_product_product.sh
bash sqoop_product_classify.sh