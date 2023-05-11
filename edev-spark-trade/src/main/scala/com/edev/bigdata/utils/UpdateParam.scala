package com.edev.bigdata.utils

@SerialVersionUID(1234567890L)
class UpdateParam extends Serializable{
  var partitionField: String = _
  var partitionType: String = _
  var partedField: String = _
  var keyField: String = _
  var schema: String = _
  var table: String = _
  var numPartitions: Int = 120
}
