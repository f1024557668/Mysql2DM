package com.data.demo;

import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;

public class TransformDM {
    public static final Set<String> KEYWORDS = new HashSet<>();
    private static final String JDBC_URL = "jdbc:mysql://xxxx:3306/%s?useAffectedRows=true&useSSL=false&useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&allowMultiQueries=true";
    public static final String ROOT = "";
    public static final String MYSQL_ZBKJ_0906 = "";

    static {
        KEYWORDS.add("path");
        KEYWORDS.add("type");
        KEYWORDS.add("version");
        KEYWORDS.add("value");
        KEYWORDS.add("grouping");
    }

    public static void main(String[] args) throws IOException {
        String schema = "";
        Result result = getResult(schema);
        printDDL(result, schema);

        generateInsert(schema);
        System.exit(0);
    }

    private static void generateInsert(String schema) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDriverClassName("com.mysql.cj.jdbc.Driver");
        hikariConfig.setJdbcUrl(String.format(JDBC_URL, schema));
        hikariConfig.setUsername(ROOT);
        hikariConfig.setPassword(MYSQL_ZBKJ_0906);
        // 设置可以获取tables remarks信息
        hikariConfig.addDataSourceProperty("useInformationSchema", "true");
        hikariConfig.setMinimumIdle(2);
        hikariConfig.setMaximumPoolSize(5);
        DataSource dataSource = new HikariDataSource(hikariConfig);
        // 用 jdbc 方式获取所有的表注释，并生成 sql 脚本
        // 遍历结果集获取表名
        Connection connection = null;
        PreparedStatement stmt = null;
        try {
            connection = dataSource.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet resultSet = metaData.getTables(connection.getCatalog(), "", null, new String[] { "TABLE" });
            try (FileWriter writer = new FileWriter("dm_" + schema + "_insert.sql")) {
                while (resultSet.next()) {
                    String tableName = resultSet.getString("TABLE_NAME");
                    stmt = connection.prepareStatement(String.format("SELECT  * FROM %s", tableName));
                    ResultSet data = stmt.executeQuery();
                    int columnCount = data.getMetaData().getColumnCount();
                    while (data.next()) {
                        // 构建字段列表
                        StringBuilder columns = new StringBuilder("(");
                        // 构建值列表
                        StringBuilder values = new StringBuilder("(");

                        for (int i = 1; i <= columnCount; i++) {
                            String colType = data.getMetaData().getColumnTypeName(i).toUpperCase();
                            Object value = data.getObject(i);

                            // 处理字段名
                            columns.append("\"").append(data.getMetaData().getColumnName(i)).append("\",");

                            // 处理字段值
                            if (value == null) {
                                values.append("NULL,");
                            } else if (colType.contains("TEXT") || colType.contains("LONGBLOB") || colType.contains("JSON")) {
                                String escaped = null;
                                if (value instanceof String) {
                                    escaped = value.toString();
                                }
                                else {
                                    escaped = (IOUtils.toString(new ByteArrayInputStream((byte[]) value), "UTF-8"))
                                            .replace("'", "''");
                                }
                                values.append("'").append(escaped).append("',");
                            }
                            else if (colType.contains("INT") || colType.contains("BIT") || colType.contains("DECIMAL")
                                    || colType.contains("LONG")) {
                                values.append(value).append(",");
                            } else {
                                values.append("'").append(value.toString().replace("'", "''")).append("',");
                            }
                        }

                        // 生成完整SQL
                        String sql = "INSERT INTO \"" + tableName + "\" "
                                + columns.deleteCharAt(columns.length() - 1).append(")  ") + "VALUES "
                                + values.deleteCharAt(values.length() - 1).append(");\n");

                        writer.write(sql);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void printDDL(Result result, String schema) {
        // 初始化模板引擎
        initVelocity();

        // 创建上下文并填充数据
        VelocityContext context = new VelocityContext();
        List<Map<String, Object>> tables = new LinkedList<>();
        result.tableMap.forEach((k, v) -> {
            Map<String, Object> datas = new HashMap<>();
            datas.put("table_name", k);
            datas.put("columns", v);
            String join = CollUtil.join(result.primaryKeyMap.get(k), ",");
            if (StrUtil.isBlank(join)) {
                join = "id";
            }
            datas.put("pk_column", join);
            datas.put("indexes", result.indexMap.get(k));
            tables.add(datas);
        });
        context.put("tables", tables);

        // StringWriter writer = new StringWriter();
        // 读取模板
        Template template = Velocity.getTemplate("ddl.vm", "UTF-8");

        // 渲染模板
        try (FileWriter fileWriter = new FileWriter("ddl_" + schema + ".sql")) {
            template.merge(context, fileWriter);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // template.merge(context, new PrintWriter(writer));
        // System.out.println(writer.toString());
    }

    private static Result getResult(String schema) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDriverClassName("com.mysql.cj.jdbc.Driver");
        hikariConfig.setJdbcUrl(String.format(JDBC_URL, schema));
        hikariConfig.setUsername(ROOT);
        hikariConfig.setPassword(MYSQL_ZBKJ_0906);
        // 设置可以获取tables remarks信息
        hikariConfig.addDataSourceProperty("useInformationSchema", "true");
        hikariConfig.setMinimumIdle(2);
        hikariConfig.setMaximumPoolSize(5);
        DataSource dataSource = new HikariDataSource(hikariConfig);
        // 用 jdbc 方式获取所有的表注释，并生成 sql 脚本
        // 遍历结果集获取表名
        Map<String, List<Map<String, Object>>> tableMap = new HashMap<>();
        Map<String, List<Map<String, String>>> indexMap = new HashMap<>();
        LinkedHashMap<String, List<String>> primaryKeyMap = new LinkedHashMap<>();
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet resultSet = metaData.getTables(connection.getCatalog(), "", null, new String[] { "TABLE" });
            List<String> tables = new ArrayList<>();
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                tables.add(tableName);
                List<Map<String, Object>> mapList = new ArrayList<>();
                ResultSet primaryKeys = metaData.getPrimaryKeys(connection.getCatalog(), "%", tableName);
                Set<String> primaryKeysSet = new HashSet<>();
                while (primaryKeys.next()) {
                    primaryKeysSet.add(primaryKeys.getString("COLUMN_NAME"));
                }
                ResultSet columns = metaData.getColumns(connection.getCatalog(), "%", tableName, "%");
                while (columns.next()) {
                    Map<String, Object> map = new HashMap<>();
                    String colName = columns.getString("COLUMN_NAME");
                    String createColName = null;
                    if (KEYWORDS.contains(colName.toLowerCase())) {
                        createColName = "\"" + colName + "\"";
                    } else {
                        createColName = colName;
                    }
                    map.put("createColName", createColName);
                    map.put("name", colName);
                    if (primaryKeysSet.contains(colName)) {
                        List<String> strings = primaryKeyMap.get(tableName);
                        if (strings == null || strings.isEmpty()) {
                            strings = new LinkedList<>();
                        }
                        if (!strings.contains(colName)) {
                            strings.add(colName);
                            primaryKeyMap.put(tableName, strings);
                        }

                        map.put("primaryKey", true);
                    } else {
                        map.put("primaryKey", false);
                    }
                    String remarks = columns.getString("REMARKS");
                    boolean nullable = columns.getBoolean("NULLABLE");
                    map.put("nullable", nullable);
                    remarks = remarks.replaceAll("\n", "").trim();
                    map.put("remark", remarks);
                    map.put("needDefault", false);
                    if (nullable) {
                        map.put("needDefault", true);
                        if ("".equals(columns.getString("COLUMN_DEF"))) {
                            map.put("default", "''");
                        } else if (columns.getString("COLUMN_DEF") == null) {
                            map.put("default", "null");
                        } else {
                            String columnDef = columns.getString("COLUMN_DEF");
                            if (columnDef.startsWith("b'")) {
                                columnDef = columnDef.substring(1);
                            }
                            map.put("default", columnDef);
                        }
                    }
                    String typeName = columns.getString("TYPE_NAME");
                    if ("BIT".equals(typeName)) {
                        typeName = "TINYINT";
                    }
                    if ("BLOB".equals(typeName) || "LONGBLOB".equals(typeName) || "JSON".equals(typeName)) {
                        typeName = "CLOB";
                    }
                    if ("TIMESTAMP".equals(typeName)) {
                        int columnSize = columns.getInt("COLUMN_SIZE");
                        if (columnSize == 19) {
                            typeName = "TIMESTAMP";
                        } else {
                            typeName = "TIMESTAMP(" + (columnSize - 20) + ")";
                        }
                    }
                    if ("VARCHAR".equals(typeName)) {
                        int columnSize = columns.getInt("COLUMN_SIZE");
                        columnSize = columnSize == 0 ? 100 : columnSize;
                        typeName = typeName + "(" + columnSize * 2 + ")";
                    }
                    map.put("type", typeName.replace("UNSIGNED", ""));

                    mapList.add(map);
                }
                tableMap.put(tableName, mapList);

                try (ResultSet indexInfo = metaData.getIndexInfo(connection.getCatalog(), null, tableName, false,
                        true)) {
                    Map<String, Map<String, String>> index = new HashMap<>();
                    while (indexInfo.next()) {
                        // 过滤统计信息（某些驱动返回额外行）
                        if (indexInfo.getShort("TYPE") == DatabaseMetaData.tableIndexStatistic) {
                            continue;
                        }

                        String indexName = indexInfo.getString("INDEX_NAME");
                        if ("PRIMARY".equals(indexName)) {
                            continue;
                        }
                        String columnName = indexInfo.getString("COLUMN_NAME");
                        boolean nonUnique = indexInfo.getBoolean("NON_UNIQUE");

                        Map<String, String> e = null;
                        if (index.containsKey(indexName)) {
                            e = index.get(indexName);
                            e.put("index_column", e.get("index_column") + ",\"" + columnName + "\"");
                        } else {
                            e = new HashMap<>();
                            e.put("index_name", indexName);
                            e.put("index_column", "\"" + columnName + "\"");
                            e.put("non_unique", String.valueOf(nonUnique));
                        }
                        index.put(indexName, e);
                    }
                    indexMap.put(tableName, new ArrayList<>(index.values()));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        Result result = new Result(tableMap, indexMap, primaryKeyMap);
        return result;
    }

    private static class Result {
        public final Map<String, List<Map<String, Object>>> tableMap;
        public final Map<String, List<Map<String, String>>> indexMap;
        public final Map<String, List<String>> primaryKeyMap;

        public Result(Map<String, List<Map<String, Object>>> tableMap,
                Map<String, List<Map<String, String>>> indexMap,Map<String, List<String>> primaryKeyMap) {
            this.tableMap = tableMap;
            this.indexMap = indexMap;
            this.primaryKeyMap = primaryKeyMap;
        }
    }

    public static void initVelocity() {
        Properties properties = new Properties();
        try {
            // 加载classpath目录下的vm文件
            properties.setProperty("resource.loader.file.class",
                    "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");
            // 定义字符集
            properties.setProperty(Velocity.INPUT_ENCODING, Charset.defaultCharset().name());
            properties.setProperty("resource.manager.cache.enabled", "true");
            properties.setProperty("file.resource.loader.cache", "true");
            // 初始化Velocity引擎，指定配置Properties
            Velocity.init(properties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
