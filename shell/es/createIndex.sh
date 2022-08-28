#!/bin/bash
host=${es_nodes}
echo "elasticSearch's host is: "$host

curl -XDELETE $host'/order.list'
curl -XPUT $host'/order.list' -d '
{
    "mappings":{
      "properties":{
        "order_key":{"type":"integer"},
        "date_key":{"type":"integer"},
        "customer_key":{"type":"integer"},
        "address_key":{"type":"integer"},
        "region_key":{"type":"integer"},
        "amount":{"type":"double"},
        "order_time":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
        "flag":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
        "customer_name":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
        "gender":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
        "birthdate":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
        "identification":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
        "phone_number":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}
      }
    }
}'

curl -XDELETE $host'/order_item.list'
curl -XPUT $host'/order_item.list' -d '
{
    "mappings":{
      "properties":{
        "order_item_key":{"type":"integer"},
        "date_key":{"type":"integer"},
        "order_key":{"type":"integer"},
        "customer_key":{"type":"integer"},
        "address_key":{"type":"integer"},
        "region_key":{"type":"integer"},
        "product_key":{"type":"integer"},
        "classify_key":{"type":"integer"},
        "supplier_key":{"type":"integer"},
        "order_time":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
        "quantity":{"type":"integer"},
        "price":{"type":"double"},
        "amount":{"type":"double"},
        "customer_name":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
        "gender":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
        "birthdate":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
        "identification":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
        "phone_number":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
        "unit":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
        "image":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},
        "original_price":{"type":"double"},
        "tip":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}
      }
    }
}'