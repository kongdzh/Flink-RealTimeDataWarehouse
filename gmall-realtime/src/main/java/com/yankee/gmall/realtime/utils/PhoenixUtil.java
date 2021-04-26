package com.yankee.gmall.realtime.utils;

import com.yankee.gmall.realtime.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PhoenixUtil {
    // 声明Connection
    private static Connection connection;

    private static Connection init() {
        try {
            Class.forName(GmallConfig.PHOENIX_DRIVER);
            Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            connection.setSchema(GmallConfig.HBASE_SCHEMA);

            return connection;
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("获取Phoenix连接失败！");
        }
    }

    public static <T> List<T> queryList(String sql, Class<T> clazz) {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        // 初始化连接
        if (connection == null) {
            connection = init();
        }

        try {
            // 编译SQL
            preparedStatement = connection.prepareStatement(sql);

            // 执行查询
            resultSet = preparedStatement.executeQuery();

            // 获取查询结果中的黎明
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            ArrayList<T> list = new ArrayList<>();
            while (resultSet.next()) {
                T t = clazz.newInstance();
                for (int i = 1; i < columnCount + 1; i++) {
                    BeanUtils.setProperty(t, metaData.getColumnName(i), resultSet.getObject(i));
                }
                list.add(t);
            }
            return list;
        } catch (SQLException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
            throw new RuntimeException("查询Phoenix维度信息失败！");
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
