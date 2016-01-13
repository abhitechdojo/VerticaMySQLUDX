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
    public void plan(ServerInterface srvInterface, NodeSpecifyingPlanContext planCtxt) throws UdfException{
        findExecutionNodes(srvInterface.getParamReader(), planCtxt, srvInterface.getCurrentNodeName(), "nodes");
    }

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
    public void getParameterType(ServerInterface srvInterface, SizedColumnTypes parameterTypes) {
        parameterTypes.addVarchar(65000, "mySqlConnectionString");
        parameterTypes.addVarchar(65000, "userName");
        parameterTypes.addVarchar(65000, "password");
        parameterTypes.addVarchar(65000, "tableName");
        parameterTypes.addVarchar(65000, "nodes");
    }

    private void findExecutionNodes(ParamReader args, NodeSpecifyingPlanContext planCtxt, String defaultList,
                                    String node_args_name) throws UdfException {
        String nodes;
        List<String> clusterNodes = planCtxt.getClusterNodes();
        List<String> executionNodes = new ArrayList<String>();

        if (args.containsParameter(node_args_name)) {
            nodes = args.getString(node_args_name);
        } else if (defaultList != "") {
            nodes = defaultList;
        } else {
            // we don't have any nodes
            return;
        }

        if (nodes == "ALL NODES") {
            executionNodes = clusterNodes;
        } else if (nodes == "ANY NODE") {
            long seconds = System.currentTimeMillis()/1000;
            Random generator = new Random(seconds);
            String tmpNode = clusterNodes.get(generator.nextInt()%clusterNodes.size());
            executionNodes.add(tmpNode);
        } else if (nodes == "") {
            return;
        } else {
            // have to actually parse the comma separated string
            String[] tokens = nodes.split(",");
            for(int i = 0; i < tokens.length; i++) {
                if (clusterNodes.contains(tokens[i])) {
                    executionNodes.add(tokens[i]);
                } else {
                    String msg = String.format("Specified node %s does not exist in the cluster. The nodes available in the cluster are \"%s\".", tokens[i], clusterNodes.toString());
                    throw new UdfException(0, msg);
                }
            }
        }
    }
}
