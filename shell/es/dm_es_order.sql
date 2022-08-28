drop table dm.dm_es_order;
create external table if not exists dm.dm_es_order(
order_key           	int,
date_key            	int,
customer_key        	int,
address_key         	int,
region_key          	int,
amount              	double,
order_time          	string,
flag                	string,
customer_name       	string,
gender                 	string,
birthdate            	string,
identification      	string,
phone_number        	string
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES('es.resource' = 'order.list','es.mapping.id' = 'order_key','es.batch.write.retry.count'='-1','es.nodes'='elastic-search');
drop table dm.dm_es_order_item;
create external table if not exists dm.dm_es_order_item(
order_item_key          	int,
date_key            	int,
order_key           	int,
customer_key        	int,
address_key         	int,
region_key          	int,
product_key        	int,
classify_key         	int,
supplier_key          	int,
order_time          	string,
quantity          	int,
price              	double,
amount              	double,
customer_name       	string,
sex                 	string,
birthday            	string,
identification      	string,
phone_number        	string,
product_name       	string,
unit      	string,
image        	string,
original_price              	double,
tip        	string
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES('es.resource' = 'order_item.list','es.mapping.id' = 'order_key','es.batch.write.retry.count'='-1','es.nodes'='elastic-search');