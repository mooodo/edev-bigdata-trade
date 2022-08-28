#!/bin/bash
cd $(pwd)
source app.conf
bash sqoop_order_order.sh
bash sqoop_order_order_item.sh