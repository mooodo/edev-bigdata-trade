#!/bin/bash
#  $1: the jar file
bash etl_address.sh $1
bash etl_classify.sh $1
bash etl_customer.sh $1
bash etl_order.sh $1
bash etl_order_item.sh $1
bash etl_product.sh $1
bash etl_region.sh $1