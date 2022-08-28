#!/bin/bash
#  $1: the jar file
bash dw_dim_date.sh $1 '2000-01' '2030-12'
bash dw_dim_address.sh $1
bash dw_dim_classify.sh $1
bash dw_dim_customer.sh $1
bash dw_dim_product.sh $1
bash dw_dim_region.sh $1
bash dw_fact_order.sh $1
bash dw_fact_order_item.sh $1
bash dw_agg_order_by_customer.sh $1
bash dw_agg_order_by_region.sh $1
bash dw_agg_order_item_by_classify.sh $1
bash dw_agg_order_item_by_product.sh $1