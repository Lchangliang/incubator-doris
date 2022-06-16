import org.codehaus.groovy.runtime.IOGroovyMethods

suite ("test_compaction_schema_change") {
    def tableName = "lineorder"
    def columns = """lo_orderkey,lo_linenumber,lo_custkey,lo_partkey,lo_suppkey,lo_orderdate,lo_orderpriority, 
                    lo_shippriority,lo_quantity,lo_extendedprice,lo_ordtotalprice,lo_discount, 
                    lo_revenue,lo_supplycost,lo_tax,lo_commitdate,lo_shipmode,lo_dummy"""
    
    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """ CREATE TABLE IF NOT EXISTS ${tableName} (
        `lo_orderkey` bigint(20) NOT NULL COMMENT "",
        `lo_linenumber` bigint(20) NOT NULL COMMENT "",
        `lo_custkey` int(11) NOT NULL COMMENT "",
        `lo_partkey` int(11) NOT NULL COMMENT "",
        `lo_suppkey` int(11) NOT NULL COMMENT "",
        `lo_orderdate` int(11) NOT NULL COMMENT "",
        `lo_orderpriority` varchar(16) NOT NULL COMMENT "",
        `lo_shippriority` int(11) NOT NULL COMMENT "",
        `lo_quantity` bigint(20) NOT NULL COMMENT "",
        `lo_extendedprice` bigint(20) NOT NULL COMMENT "",
        `lo_ordtotalprice` bigint(20) NOT NULL COMMENT "",
        `lo_discount` bigint(20) NOT NULL COMMENT "",
        `lo_revenue` bigint(20) NOT NULL COMMENT "",
        `lo_supplycost` bigint(20) NOT NULL COMMENT "",
        `lo_tax` bigint(20) NOT NULL COMMENT "",
        `lo_commitdate` bigint(20) NOT NULL COMMENT "",
        `lo_shipmode` varchar(11) NOT NULL COMMENT ""
        )
        PARTITION BY RANGE(`lo_orderdate`)
        (PARTITION p1992 VALUES [("-2147483648"), ("19930101")),
        PARTITION p1993 VALUES [("19930101"), ("19940101")),
        PARTITION p1994 VALUES [("19940101"), ("19950101")),
        PARTITION p1995 VALUES [("19950101"), ("19960101")),
        PARTITION p1996 VALUES [("19960101"), ("19970101")),
        PARTITION p1997 VALUES [("19970101"), ("19980101")),
        PARTITION p1998 VALUES [("19980101"), ("19990101")))
        DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 1
        PROPERTIES (
        "replication_num" = "1"
        );"""

    Thread thread = new Thread(() -> {
        streamLoad {
            // a default db 'regression_test' is specified in
            // ${DORIS_HOME}/conf/regression-conf.groovy
            table tableName

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', '|'
            set 'compress_type', 'GZ'
            set 'columns', columns
            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file """${getS3Url() + '/regression/ssb/sf1/' + tableName}.tbl.gz"""

            time 10000 // limit inflight 10s

            // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    })
        
    Thread.sleep(60000)

    sql """ alter table ${tableName} add column test_column1 int default '1'; """
    sql """ insert into ${tableName} values (-1, 1, 1, 1, 1, 1, '1', 1, 1, 1, 1, 1, 1, 1, 1, 1, '1', 100); """
    qt_compaction """ select test_column1 from ${tableName} where lo_orderkey = -1; """
    
    thread.join()

    // wait to auto compaction
    Thread.sleep(60000)

    qt_compaction """ select test_column1 from ${tableName} where lo_orderkey = -1; """ 

    sql """ alter table ${tableName} add column test_column2 varchar(16) default '1' after lo_partkey; """
    sql """ insert into ${tableName} values (-1, 1, 1, 1, '2', 1, 1, '1', 1, 1, 1, 1, 1, 1, 1, 1, 1, '1', 100); """

    qt_compaction """ select test_column2 from ${tableName} where lo_orderkey = -1; """
}
