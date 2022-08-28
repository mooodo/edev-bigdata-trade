set hive.auto.convert.join=false;
insert into dm.dm_hbase_customer
select customer_key rowkey,map(
    'customer_key',COALESCE(customer_key,''),
    'name',COALESCE(name,''),
    'gender',COALESCE(gender,''),
    'birthdate',COALESCE(birthdate,''),
    'identification',COALESCE(identification,''),
    'phone_number',COALESCE(phone_number,'')
) customer,
map() vip,
map() account,
from_utc_timestamp(CURRENT_TIMESTAMP,'GMT+8') update_time
from dw.dw_dim_customer;
set hive.auto.convert.join=true;