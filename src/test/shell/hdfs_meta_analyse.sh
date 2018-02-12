#!/bin/bash

set -o nounset
#set -o errexit
set -e

###############################################################################
# 脚本名称  : generate_hdfs_small_files_report.sh
# 功能描述  : 针对HDFS存储的小文件进行统计，为后续小文件优化做准备
# 输入参数  : NO
# 输出参数  : NO
# 返 回 值  : SUCESS:0,FALSE:1
# 调用函数  : 无
# 被调函数  : 无
# 修改历史  :
# 1.日    期  :
#   作    者  :
#   修改内容  :
###############################################################################


###############################################################################
# 函数名称  : kinit_user_keytab
# 功能描述  : 进行Kerberos认证
# 输入参数  : user_keytab_file
# 输出参数  : NO
# 返 回 值  : SUCESS:0,FALSE:1
# 调用函数  : 无
# 被调函数  : main
# 修改历史  :
# 1.日    期  :
#   作    者  :
#   修改内容  :
###############################################################################
kinit_user_keytab()
{
    t_user_keytab_file=$1
    user_name=$2

    # kinit hdfs keytab
    if [ -f ${t_user_keytab_file} ]
    then
        kinit -kt ${t_user_keytab_file} ${user_name}
    else
        echo "The file named ${t_user_keytab_file} is not found..."
        exit 1
    fi

}

###############################################################################
# 函数名称  : prepare_operation
# 功能描述  : 创建临时目录，删除历史数据等操作
# 输入参数  : t_save_fsimage_path
# 输出参数  : NO
# 返 回 值  : SUCESS:0,FALSE:1
# 调用函数  : 无
# 被调函数  : main
# 修改历史  :
# 1.日    期  :
#   作    者  :
#   修改内容  :
###############################################################################
prepare_operation()
{
    # get parameters
    t_save_fsimage_path=$1

    # delete history fsimage
    #fsimage_tmp_file=`ls ${t_save_fsimage_path}/fsimage*`
    fsimage_tmp_file=`find ${t_save_fsimage_path} -name "fsimage*"`
    if [ ! -z "${fsimage_tmp_file}" ]
    then
        for file in ${fsimage_tmp_file}
        do
            rm -f ${file}
        done
    fi


    hadoop fs -test -e ${t_save_fsimage_path}/hive_table_info || hdfs dfs -mkdir -p ${t_save_fsimage_path}/hive_table_info
    # 使用set -e时，如果命令返回结果不为0就报错，即无法再使用$?获取命令结果，可用||或!处理
    #if [ $? -ne 0 ]
    #then
    #    hdfs dfs -mkdir -p ${t_save_fsimage_path}/hive_table_info
    #fi

}

###############################################################################
# 函数名称  : get_hive_metadata
# 功能描述  : 获取Hive的元数据
# 输入参数  : t_save_fsimage_path
# 输出参数  : NO
# 返 回 值  : SUCESS:0,FALSE:1
# 调用函数  : 无
# 被调函数  : main
# 修改历史  :
# 1.日    期  :
#   作    者  :
#   修改内容  :
###############################################################################
get_hive_metadata()
{
    # 获取传入参数
    t_save_fsimage_path=$1

    # 创建视图(MariaDB数据库中已经存在，注释掉)
    #create or replace view v_hive_metastore.v_table_information
    #as
    #select db.NAME db_name,
    #       tbl.TBL_NAME tbl_name,
    #       tbl.OWNER tbl_owner,
    #       sds.LOCATION location,
    #       sds.OUTPUT_FORMAT output_format
    # from metastore.DBS db
    #join metastore.TBLS tbl
    # on db.DB_ID = tbl.DB_ID
    #join metastore.SDS sds
    # on tbl.SD_ID = sds.SD_ID;

    # 升级上面的Hive表的元数据信息(废弃)
    #create or replace view v_hive_metastore.v_table_information
    #as
    #select db.NAME db_name,
    #       tbl.TBL_NAME tbl_name,
    #       tbl.OWNER tbl_owner,
    #       sds.LOCATION location,
    #       partitions.PART_NAME partition_name,
    #       sds.INPUT_FORMAT input_format,
    #       sds.OUTPUT_FORMAT output_format,
    #       sds.IS_COMPRESSED is_compressed
    # from metastore.DBS db
    #join metastore.TBLS tbl
    # on db.DB_ID = tbl.DB_ID
    #join metastore.SDS sds
    # on tbl.SD_ID = sds.SD_ID
    #left join metastore.PARTITIONS partitions
    # on tbl.TBL_ID = partitions.TBL_ID;

    # 导出Hive元数据内容
    # mysql -u *** -p123456 -h *** -e "select * into outfile '/tmp/v_table_information.csv' fields terminated by ',' from v_hive_metastore.v_table_information;"
    mysql -u *** -p123456 -h *** -Ne "use v_hive_metastore; select * from v_hive_metastore.v_table_information;" > ${t_save_fsimage_path}/v_table_information.csv

    # 将Hive元数据导入到HDFS指定目录
    hdfs dfs -copyFromLocal -f ${t_save_fsimage_path}/v_table_information.csv ${t_save_fsimage_path}/hive_table_info/

}

###############################################################################
# 函数名称  : get_hdfs_fsimage
# 功能描述  : 获取HDFS的FSImage
# 输入参数  : t_save_fsimage_path
# 输出参数  : NO
# 返 回 值  : SUCESS:0,FALSE:1
# 调用函数  : 无
# 被调函数  : main
# 修改历史  :
# 1.日    期  :
#   作    者  :
#   修改内容  :
###############################################################################
get_hdfs_fsimage()
{
    # 进行HDFS的Kerberos认证
    kinit_user_keytab ${hdfs_keytab_file} hdfs

    # 获取传入参数
    t_save_fsimage_path=$1

    # 从namenode上下载fsimage
    hdfs dfsadmin -fetchImage ${t_save_fsimage_path}

    # 获取下载的fsimage具体文件路径
    t_fsimage_file=`ls ${t_save_fsimage_path}/fsimage*`

    # 处理fsimage为可读的csv格式文件
    hdfs oiv -i ${t_fsimage_file} -o ${t_save_fsimage_path}/fsimage.csv -p Delimited

    # 删除fsimage.csv的首行数据
    sed -i -e "1d" ${t_save_fsimage_path}/fsimage.csv

    # 进行的用户的Kerberos认证
    kinit_user_keytab ${user_keytab_file} ***

    # 创建数据目录
    hadoop fs -test -e ${t_save_fsimage_path}/fsimage || hdfs dfs -mkdir -p ${t_save_fsimage_path}/fsimage

    # 拷贝fsimage.csv到指定的路径
    hdfs dfs -copyFromLocal -f ${t_save_fsimage_path}/fsimage.csv ${t_save_fsimage_path}/fsimage/

}

###############################################################################
# 函数名称  : create_impala_table
# 功能描述  : 创建存储Hive和HDFS元数据的临水表和处理的结果表
# 输入参数  : NO
# 输出参数  : NO
# 返 回 值  : SUCESS:0,FALSE:1
# 调用函数  : 无
# 被调函数  : 无
# 修改历史  :
# 1.日    期  :
#   作    者  :
#   修改内容  :
###############################################################################
create_impala_table()
{
    # 创建存储Hive元数据的表
    #impala-shell -i ***:21000 -l --auth_creds_ok_in_clear -u *** --ldap_password_cmd="printf ***" -q "
    #use idc_infrastructure_db;
    #CREATE EXTERNAL TABLE idc_infrastructure_db.hive_table_info (
    # db_name STRING ,
    # tbl_name STRING ,
    # tbl_owner STRING ,
    # table_location STRING)
    #row format delimited
    #fields terminated by '\t'
    #LOCATION '${t_save_fsimage_path}/hive_table_info';"

    # 升级存储Hive元数据的表(废弃)
    #impala-shell -i ***:21000 -l --auth_creds_ok_in_clear -u *** --ldap_password_cmd="printf ***" -q "
    #use idc_infrastructure_db;
    #CREATE EXTERNAL TABLE idc_infrastructure_db.hive_table_info (
    # db_name STRING ,
    # tbl_name STRING ,
    # tbl_owner STRING ,
    # table_location STRING,
    # partition_name STRING,
    # input_format STRING,
    # output_format STRING,
    # is_compressed int)
    #row format delimited
    #fields terminated by '\t'
    #LOCATION '${t_save_fsimage_path}/hive_table_info';"

    # 在Impala中创建存储HDFS的fsimage的外部表
    #impala-shell -i ***:21000 -l --auth_creds_ok_in_clear -u *** --ldap_password_cmd="printf ***" -q "
    #use idc_infrastructure_db;
    #CREATE EXTERNAL TABLE idc_infrastructure_db.HDFS_META_D (
    # PATH STRING ,
    # REPL INT ,
    # MODIFICATION_TIME STRING ,
    # ACCESSTIME STRING ,
    # PREFERREDBLOCKSIZE INT ,
    # BLOCKCOUNT DOUBLE,
    # FILESIZE DOUBLE ,
    # NSQUOTA INT ,
    # DSQUOTA INT ,
    # PERMISSION STRING ,
    # USERNAME STRING ,
    # GROUPNAME STRING)
    #row format delimited
    #fields terminated by '\t'
    #LOCATION '${t_save_fsimage_path}/fsimage';"

    # 在Impala创建HDFS的fsimage最终处理的结果表
    #impala-shell -i ***:21000 -l --auth_creds_ok_in_clear -u *** --ldap_password_cmd="printf ***" -q "
    #CREATE TABLE idc_infrastructure_db.HDFS_META (
    # PATH STRING ,
    # REPL INT ,
    # MODIFICATION_TIME TIMESTAMP ,
    # ACCESSTIME TIMESTAMP ,
    # PREFERREDBLOCKSIZE INT ,
    # BLOCKCOUNT DOUBLE,
    # FILESIZE DOUBLE ,
    # NSQUOTA INT ,
    # DSQUOTA INT ,
    # PERMISSION STRING ,
    # USERNAME STRING ,
    # GROUPNAME STRING)
    #STORED AS PARQUETFILE;"

    # 加载HDFS的fsimage到结果表中
    impala-shell -i ***:21000 -l --auth_creds_ok_in_clear -u *** --ldap_password_cmd="printf ***" -q "
    INVALIDATE METADATA idc_infrastructure_db.hdfs_meta_d;
    INVALIDATE METADATA idc_infrastructure_db.hdfs_meta;
    INSERT OVERWRITE idc_infrastructure_db.hdfs_meta
    SELECT
     PATH,
     REPL,
     cast(concat(MODIFICATION_TIME,':00') as timestamp),
     cast(concat(ACCESSTIME,':00') as timestamp),
     PREFERREDBLOCKSIZE,
     BLOCKCOUNT,
     FILESIZE,
     NSQUOTA,
     DSQUOTA,
     PERMISSION,
     USERNAME,
     GROUPNAME
    FROM
     idc_infrastructure_db.hdfs_meta_d;

    invalidate metadata idc_infrastructure_db.hdfs_meta;"

}

###############################################################################
# 函数名称  : get_hdfs_statistics_info
# 功能描述  : 获取HDFS的统计信息(包括小文件等)
# 输入参数  : NO
# 输出参数  : NO
# 返 回 值  : SUCESS:0,FALSE:1
# 调用函数  : 无
# 被调函数  : 无
# 修改历史  :
# 1.日    期  :
#   作    者  :
#   修改内容  :
###############################################################################
get_hdfs_statistics_info()
{
    # 同步表数据
    impala-shell -i ***:21000 -l --auth_creds_ok_in_clear -u *** --ldap_password_cmd="printf ***" -q "
    refresh idc_infrastructure_db.hive_table_info;
    "

    # 每天创建的文件数和大小
    #impala-shell -i ***:21000 -l --auth_creds_ok_in_clear -u *** --ldap_password_cmd="printf ***" -q "
    #select trunc(modification_time,'DD') day,count(1) num_of_files, round(sum(filesize)/1024/1024/1024,2) Total_Size_GB from idc_infrastructure_db.hdfs_meta group by trunc(modification_time,'DD') order by trunc(modification_time,'DD');"

    # 在Impala创建HDFS统计小文件的表
    #CREATE TABLE idc_infrastructure_db.hdfs_small_files_result (
    #  db_name STRING,
    #  tbl_name STRING,
    #  tbl_owner STRING,
    #  table_location STRING,
    #  storage_format STRING,
    #  file_size_type STRING,
    #  small_files_count BIGINT,
    #  support_person STRING
    #)
    #STORED AS PARQUETFILE;

    # 解决Impala删除表后操作
    impala-shell -i ***:21000 -l --auth_creds_ok_in_clear -u *** --ldap_password_cmd="printf ***" -q "
    use idc_infrastructure_db;
    refresh idc_infrastructure_db.hdfs_meta;
    INSERT OVERWRITE idc_infrastructure_db.hdfs_small_files_result
    select t.db_name,t.tbl_name,t.tbl_owner,t.table_location,t.storage_format,t.file_size_type,count(t.file_size_type) small_files_count,t.support_person
    from (
        select hive.db_name,
           hive.tbl_name,
           hive.tbl_owner,
           hive.table_location,
           case when output_format rlike 'Text' then 'TextFile'
                when output_format rlike 'Parquet' then 'Parquet'
                when output_format rlike 'SequenceFile' then 'SequenceFile'
                when output_format rlike 'HiveHFile' then 'HiveHFile'
                when output_format rlike 'Orc' then 'HiveHFile'
             else 'OtherFormat' end storage_format,
           hdfs.path,
           case when round(hdfs.filesize/1024/1024,2) < 0.1 then 'less_100K'
                when round(hdfs.filesize/1024/1024,2) >= 0.1 and round(hdfs.filesize/1024/1024,2) < 1 then 'between_100K_and_1M'
                when round(hdfs.filesize/1024/1024,2) >= 1 and round(hdfs.filesize/1024/1024,2) <= 10 then 'between_1M_and_10M'
           else 'omit' end file_size_type,

           case when hive.db_name in ('idc_modeling','wifilog') then 'fanhouli'
                when hive.db_name = 'staging_wifilog' then 'tianjie12'
                when hive.db_name in ('staging_bill99_edw','app_ffan','sor')  then 'zhaoxudong5'
           else 'Others' end support_person
        from idc_infrastructure_db.hdfs_meta hdfs
        join idc_infrastructure_db.hive_table_info hive
        on instr(concat('hdfs://nn-idc',hdfs.path),hive.table_location) = 1
        where round(hdfs.filesize/1024/1024,2) > 0
          and round(hdfs.filesize/1024/1024,2) <= 10
          and hive.table_location != 'NULL'
     ) t
    group by t.db_name,t.tbl_name,t.tbl_owner,t.table_location,t.storage_format,t.file_size_type,t.support_person
    order by count(t.file_size_type) desc;"
}

###############################################################################
# 函 数 名  ：main
# 功能描述  ：主函数
# 输入参数  ：NO
# 输出参数  ：NO
# 返 回 值  : SUCESS:0,FALSE:1
# 调用函数  ：无
# 被调函数  ：无
# 修改历史  ：
# 1.日    期  ：
#   作    者  ：
#   修改内容  ：
###############################################################################
main()
{
    # 开始时间
    begin_time=`date +%s`

    # 定义本地和HDFS的临时目录路径
    t_save_fsimage_path=/tmp/hdfs_small_files_system

    # 定义HDFS的keytab路径
    hdfs_keytab_file=/var/lib/hadoop-hdfs/hdfs.keytab
    user_keytab_file=/var/lib/hadoop-hdfs/***.keytab

     # 进行HDFS的Kerberos认证
    kinit_user_keytab ${user_keytab_file} ***

    # 创建临时目录，删除历史数据等操作
    prepare_operation ${t_save_fsimage_path}

    # 获取Hive的元数据
    hive_metadata_update_time=`date "+%Y-%m-%d %H:%M:%S"`
    get_hive_metadata ${t_save_fsimage_path}

    # 获取HDFS的FSImage
    hdfs_fsimage_update_time=`date "+%Y-%m-%d %H:%M:%S"`
    get_hdfs_fsimage ${t_save_fsimage_path}

    # 创建存储Hive和HDFS元数据的临水表和处理的结果表
    create_impala_table

    # 获取HDFS的统计信息(包括小文件等)
    get_hdfs_statistics_info

    # 结束时间
    end_time=`date +%s`

    # 耗时(秒数)
    result_time=$((end_time-begin_time))

    echo "******************************************************************"
    echo "The script has taken ${result_time} seconds..."
    echo "Result Table: idc_infrastructure_db.hdfs_small_files_result"
    echo "HDFS FSImage update-time before: ${hdfs_fsimage_update_time}"
    echo "Hive Metadata update-time before: ${hive_metadata_update_time}"
    echo "******************************************************************"

}

#执行主方法
main "$@"
