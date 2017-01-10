package com.hadoop.test;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


public class HbaseTest {

    private static final TableName TABLE_NAME = TableName.valueOf("testtable1");

    //create hbase table if not exists
    public static void createTable(Connection hbaseConn) {
        try(Admin admin = hbaseConn.getAdmin()) {
            if(admin.tableExists(TABLE_NAME)) {
                System.out.println(TABLE_NAME.getNameAsString() + " already exists");
            } else {
                HTableDescriptor tableDesc = new HTableDescriptor(TABLE_NAME);
                tableDesc.addFamily(new HColumnDescriptor("cf1"));

                System.out.println("Creating table " + tableDesc.getNameAsString());
                admin.createTable(tableDesc);
                System.out.println("Done...");
            }

        } catch (IOException e) {
            System.out.println("ERROR: fail to create table");
            e.printStackTrace();
        }
    }


    //add row to table
    public static void addRow(Connection hbaseConn) {
        try(Table table = hbaseConn.getTable(TABLE_NAME)) {
            System.out.println("Adding row to table " + TABLE_NAME.getNameAsString());
            Long currEpoch = System.currentTimeMillis();


            Put newRow = new Put(Bytes.toBytes(currEpoch));
            newRow.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("id"),Bytes.toBytes(currEpoch));
            Date currDT = new Date();
            DateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            newRow.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("created_dttm"),Bytes.toBytes(dateFmt.format(currDT)));
            System.out.println("\t"+newRow.toString());
            table.put(newRow);
            table.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //add rows to table
    public static void addRow(Connection hbaseConn, int numRows) {
        //TODO add list for multiple puts
        if(numRows<=0) {
            numRows=1;
        }

        for(int i=0; i<numRows; i++) {
            addRow(hbaseConn);
        }

    }

    //scan table
    public static byte[] scanTable(Connection hbaseConn, int numRows) {

        byte[] rowKey=null;

        if(numRows<=0) {
            numRows=1;
        }

        try(Table table = hbaseConn.getTable(TABLE_NAME)) {
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("id"));

            try(ResultScanner tabScanner = table.getScanner(scan)) {
                int i=0;
                for(Result result= tabScanner.next(); result != null ; result=tabScanner.next(), i++ ) {
                    rowKey= result.getRow();
                    System.out.println("row: " + Bytes.toLong(result.getRow()));

                    if(i>=numRows) break;
                }
            }

        } catch(IOException e) {
            e.printStackTrace();
        }

        return rowKey;


    }

    public static void getRow(Connection hbaseConn, byte [] rowKey) {
        try( Table table = hbaseConn.getTable(TABLE_NAME)) {
            Get g = new Get(rowKey);
            Result result = table.get(g);

            if(result.isEmpty()) {
                System.out.println("Get row of " + g.toString() + " return empty");
            }
            else {
                System.out.println("Created date for row " + Bytes.toLong(g.getRow()) + " is " +
                    Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("created_dttm"))) );
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void delRow(Connection hbaseConn, byte[] rowKey) {
        try( Table table = hbaseConn.getTable(TABLE_NAME)) {
            Delete d = new Delete(rowKey);

            System.out.println("Deleting row " + Bytes.toLong(d.getRow()));
            table.delete(d);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    //delete hbase table if not exists
    public static void dropTable(Connection hbaseConn) {
        try(Admin admin = hbaseConn.getAdmin()) {
            System.out.println("Dropping " + TABLE_NAME.getNameAsString());
            admin.disableTable(TABLE_NAME);
            admin.deleteTable(TABLE_NAME);
        } catch (IOException e) {
            System.out.println("ERROR: fail to drop table");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();



        System.out.println("--- Connection Info ---");
        System.out.println("Zookeeper: " + config.get("hbase.zookeeper.quorum")+":"+config.get("hbase.zookeeper.property.clientPort"));
        System.out.println("zookeeper.session.timeout: " + config.get("zookeeper.session.timeout"));
        System.out.println("hbase.rpc.timeout: " + config.get("hbase.rpc.timeout"));
        System.out.println("--- End Connection Info ---\n\n");

        try(Connection hbaseConn = ConnectionFactory.createConnection(config)) {

            System.out.println("--- INFO: create table");
            createTable(hbaseConn);
            System.out.println("--- INFO: end of create table \n\n");

            System.out.println("--- INFO: add row");
            addRow(hbaseConn,5);
            System.out.println("--- INFO: end of add row \n\n");

            System.out.println("--- INFO: scan table");
            byte [] rowKey=scanTable(hbaseConn,10);
            System.out.println("--- INFO: end of scan table \n\n");

            if(rowKey!=null || rowKey.length>0) {
                System.out.println("--- INFO: get row");
                getRow(hbaseConn,rowKey);
                System.out.println("--- INFO: end of get row \n\n");

                System.out.println("--- INFO: delete row");
                getRow(hbaseConn,rowKey);
                System.out.println("--- INFO: end of delete row \n\n");

            }

            System.out.println("--- INFO: drop table");
            dropTable(hbaseConn);
            System.out.println("--- INFO: end of drop table \n\n");


        } catch (IOException e) {
            e.printStackTrace();
        }




    }
}
;