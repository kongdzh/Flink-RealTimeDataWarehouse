package com.yankee.gmall.realtime.common;

public class GmallConfig {
    // Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL_REALTIME";

    // Phoenix连接驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop01,hadoop02,hadoop03:2181/hbase";
}
