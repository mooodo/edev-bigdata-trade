#!/bin/bash
cd $(pwd)
source app.conf
bash sqoop_customer_customer.sh
bash sqoop_customer_address.sh
bash sqoop_customer_country.sh
bash sqoop_customer_province.sh
bash sqoop_customer_city.sh
bash sqoop_customer_zone.sh