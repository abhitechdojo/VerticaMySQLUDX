package com.abhi;

/**
 * Created by abhishek.srivastava on 1/5/16.
 */


import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import com.vertica.sdk.NodeSpecifyingPlanContext;
import com.vertica.sdk.ParamReader;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.SizedColumnTypes;
import com.vertica.sdk.SourceFactory;
import com.vertica.sdk.UDSource;
import com.vertica.sdk.UdfException;

public class VerticaMySQLUDXFactory extends SourceFactory {

    @Override
    public ArrayList<UDSource> prepareUDSources(ServerInterface srvInterface, NodeSpecifyingPlanContext planCtxt) throws UdfException {
        ArrayList<UDSource> retVal = new ArrayList<UDSource>();
        String mySqlConnectionString = srvInterface.getParamReader().getString("mySqlConnectionString");
        String userName = srvInterface.getParamReader().getString("userName");
        String password = srvInterface.getParamReader().getString("password");
        String tableName = srvInterface.getParamReader().getString("tableName");
        retVal.add(new VerticaMySQLUDX(mySqlConnectionString, tableName, userName, password));
        return retVal;
    }

    @Override
    public void plan(ServerInterface srvInterface, NodeSpecifyingPlanContext planCtx) throws UdfException {
        ArrayList<String> executionNodes = new ArrayList<String>();
        executionNodes.add(srvInterface.getCurrentNodeName());
        planCtx.setTargetNodes(executionNodes);
    }

    @Override
    public void getParameterType(ServerInterface srvInterface, SizedColumnTypes parameterTypes) {
        parameterTypes.addVarchar(65000, "mySqlConnectionString");
        parameterTypes.addVarchar(65000, "userName");
        parameterTypes.addVarchar(65000, "password");
        parameterTypes.addVarchar(65000, "tableName");
        parameterTypes.addVarchar(65000, "nodes");
    }

}
