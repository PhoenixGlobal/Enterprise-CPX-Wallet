NEXSUS Hbase Table Description
===============================
##Zoho
####ZohoModules
    描述：用于存储每个token对应的content内容.
     rowkey: token_content
####ZohoStatus
    描述: 存储token获取content后.通过content获取的status信息。这些信息主要包括:leads, campains等..
      rowkey: token_content_Id(此id为每个获得数据的唯一标识)
####ZohoIndex
     描述: 记录每个token获取每条status数据的时间，用于排序 
     rowkey: token
     content: Id (qualifier) -> time(value)
     更新方式:每次actor获取status信息时,根据status的modified time和created time更新时间
####Zoholabels(可废弃)
     描述：根据不同的content存储对应的tag标签，由于目前只针对单用户，没有区分token
##Prism
####prism_realtime_data
    描述: 从prism获取的数据。
    rowkey:token_tid_type_timestamp
    更新方式： 通过kafka consumer获取
##Label
####Label
   描述： 根据不同用户存储prism和zoho两张表的展示标签名
   rowkey: token_实际表名_逻辑表名
####LabelManager
    描述: 记录每个token对应的数据源，用于管理不同数据源的标签
    rowkey : token
    column: 不同的数据源
===============================
##Hbase 使用用可以优化的地方:
	比较重要的配置：
    		Zookeeper:     zookeeper.session.timeout： 检测某个节点是否挂了的时间间隔。
    		HDFS Configuration : hbase.regionserver.handler.count : 设置接收获取请求进程的数量.
            hbase的region分割: 
            比较重要的配置---->  hbase.regionserver.region.split.policy， hbase.hregion.max.filesize，hbase.regionserver.regionSplitLimit
            一般hbase都是自动设置分割的，当region的数量超过hbase.hregion.max.filesize时，就自动分割。可以设置这个值(hbase.hregion.max.filesize)为非常大的值，取消自动分割
