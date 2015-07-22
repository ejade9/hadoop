package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import java.util.ArrayList;

public class EzcopyCommand extends DatanodeCommand{
    public ArrayList<ExtendedBlock> srcList;
    public ArrayList<ExtendedBlock> dstList;

    public EzcopyCommand(int action, ArrayList<ExtendedBlock> srcList, ArrayList<ExtendedBlock> dstList) {
        super(action);
        this.srcList = new ArrayList<>();
        this.dstList = new ArrayList<>();
        for (ExtendedBlock b : srcList)
            this.srcList.add(b);
        for (ExtendedBlock b : dstList)
            this.dstList.add(b);
    }
}
