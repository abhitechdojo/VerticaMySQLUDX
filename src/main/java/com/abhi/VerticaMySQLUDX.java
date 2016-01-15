package com.abhi;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.vertica.sdk.DataBuffer;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.State.StreamState;
import com.vertica.sdk.UDSource;
import com.vertica.sdk.UdfException;

import java.sql.*;

public class VerticaMySQLUDX extends UDSource {
    private String mySqlConnectionString;
    private String userName;
    private String password;
    private String tableName;
    private String driver = "com.mysql.jdbc.Driver";
    private Connection mySqlConn;
    private ResultSet rs;
    private Statement stmt;
    private int resultSetColumnCount;

    public VerticaMySQLUDX(String connectionString, String tableName, String userName, String password) {
        super();
        this.mySqlConnectionString = connectionString;
        this.userName = userName;
        this.password = password;
        this.tableName = tableName;
    }

    @Override
    public void setup(ServerInterface srvInterface) throws UdfException {
        try {
            Class.forName(driver);
            mySqlConn = DriverManager.getConnection(mySqlConnectionString, userName, password);
            stmt = mySqlConn.createStatement();
            String query = "select * from " + tableName;
            rs = stmt.executeQuery(query);
            srvInterface.log("going to execute query: " + query);
            resultSetColumnCount = rs.getMetaData().getColumnCount();
            srvInterface.log("number of columns in the results: " + resultSetColumnCount);
        }
        catch(ClassNotFoundException cnfe) {
            throw new UdfException(0, "could not load the jdbc driver");
        }
        catch(SQLException sqlEx) {
            throw new UdfException(0, sqlEx.getMessage(), sqlEx);
        }
    }

    @Override
    public void destroy(ServerInterface srvInterface) throws UdfException {
        srvInterface.log("came inside destry");
        try {
            if (rs != null) {
                rs.close();
                rs = null;
            }

            if (stmt != null) {
                stmt.close();
                stmt = null;
            }

            if (mySqlConn != null) {
                mySqlConn.close();
                mySqlConn = null;
            }
        }
        catch(Exception ex) {
            throw new UdfException(0, ex.getMessage(), ex);
        }
        srvInterface.log("closed all resources successfully");
    }

    @Override
    public StreamState process(ServerInterface srvInterface, DataBuffer output) throws UdfException {
        long offset;
        srvInterface.log("total size of buffer " + output.buf.length);
        StringBuilder builder = new StringBuilder();
        try {
            if (rs.next()) {
                for(int i = 1 ; i <= resultSetColumnCount; i++) {
                    builder.append(rs.getString(i));
                    if (i < resultSetColumnCount) {
                        builder.append("|");
                    }
                }

                String row = builder.toString();
                srvInterface.log("got this row from db: " + row);
                byte[] bytes = row.getBytes();
                srvInterface.log("current value of offset " + output.offset);
                srvInterface.log("current length of buffer " + output.buf.length);

                System.arraycopy(bytes, 0, output.buf, 0, bytes.length);
                output.offset += bytes.length;
                srvInterface.log("current value of offset " + output.offset);
                srvInterface.log("current value of buffer " + output.buf);
                return StreamState.OUTPUT_NEEDED;
            } else {
                srvInterface.log("came inside the function but there is no data in resultset");
                return StreamState.DONE;
            }
        }
        catch(SQLException sqlEx) {
            throw new UdfException(0, sqlEx.getMessage(), sqlEx);
        }
    }
}
