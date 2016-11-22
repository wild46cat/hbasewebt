package com.xueyoucto.hbasett;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by Administrator on 2016-11-22.
 */
@WebServlet(name = "Servlet",urlPatterns = "/HBaseServlet")
public class Servlet extends HttpServlet {
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        doGet(request,response);
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        String tableName = "test";
        String columnFamily = "cf";
        try {

            if (true == Servlet.delete(tableName)) {
                System.out.println("Delete Table " + tableName + " success!");

            }
            System.out.println("************start create table**********");
            Servlet.create(tableName, columnFamily);
            Servlet.put(tableName, "row1", columnFamily, "column1",
                    "data1");
            Servlet.put(tableName, "row2", columnFamily, "column2",
                    "data2");
            Servlet.put(tableName, "row3", columnFamily, "column3",
                    "data3");
            Servlet.put(tableName, "row4", columnFamily, "column4",
                    "data4");
            Servlet.put(tableName, "row5", columnFamily, "column5",
                    "data5");

            Servlet.get(tableName, "row1");

            Servlet.scan(tableName);

        } catch (Exception e) {
            e.printStackTrace();
        }

        response.setCharacterEncoding("utf-8");
        response.setContentType("text/html;charset=utf-8");
        PrintWriter out = response.getWriter();
        out.println("hello world");
        out.flush();
        out.close();
    }

    static Configuration cfg;

    static {
        cfg = HBaseConfiguration.create();
        System.out.println(cfg.get("hbase.master"));
    }

    public static void create(String tableName, String columnFamily)
            throws Exception {
        HBaseAdmin admin = new HBaseAdmin(cfg);
        if (admin.tableExists(tableName)) {
            System.out.println(tableName + " exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDesc);
            System.out.println(tableName + " create successfully!");
        }
    }

    public static void put(String tablename, String row, String columnFamily,
                           String column, String data) throws Exception {

        HTable table = new HTable(cfg, tablename);
        Put put = new Put(Bytes.toBytes(row));

        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
                Bytes.toBytes(data));

        table.put(put);

        System.out.println("put '" + row + "', '" + columnFamily + ":" + column
                + "', '" + data + "'");

    }

    public static void get(String tablename, String row) throws Exception {
        HTable table = new HTable(cfg, tablename);
        Get get = new Get(Bytes.toBytes(row));
        Result result = table.get(get);
        System.out.println("Get: " + result);
    }

    public static void scan(String tableName) throws Exception {

        HTable table = new HTable(cfg, tableName);
        Scan s = new Scan();
        ResultScanner rs = table.getScanner(s);

        for (Result r : rs) {
            System.out.println("Scan: " + r);

        }
    }

    public static boolean delete(String tableName) throws IOException {

        HBaseAdmin admin = new HBaseAdmin(cfg);
        if (admin.tableExists(tableName)) {
            try {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;

    }
}
