package org.apache.hadoop.hdfs.server.protocol;

import java.util.ArrayList;
import java.util.List;

public class EzcopyCommand extends DatanodeCommand{
    public ArrayList<String> srcList;
    public ArrayList<String> dstList;
    public ArrayList<Long> offsetList;
    public ArrayList<Long> lengthList;
    public ArrayList<String> concatList;
    public ArrayList<Long> concatnum;
    public ArrayList<Long> blocksize;

    public EzcopyCommand(int action, ArrayList<String> srcList, ArrayList<String> dstList,  ArrayList<Long> offset,
                          ArrayList<Long> length, ArrayList<String> concat, ArrayList<Long> concatnum, ArrayList<Long> blocksize) {
        super(action);
        this.srcList = new ArrayList<>();
        this.dstList = new ArrayList<>();
        this.offsetList = new ArrayList<>();
        this.lengthList = new ArrayList<>();
        this.concatList = new ArrayList<>();
        this.concatnum = new ArrayList<>();
        this.blocksize = new ArrayList<>();
        for (String s : srcList)
            this.srcList.add(s);
        for (String s : dstList)
            this.dstList.add(s);
        for (Long l : offset)
            this.offsetList.add(l);
        for (Long l : length)
            this.lengthList.add(l);
        for (String s : concat)
            this.concatList.add(s);
        for (Long l : concatnum)
            this.concatnum.add(l);
        for (Long l : blocksize)
            this.blocksize.add(l);
    }
    public EzcopyCommand(int action, List<String> srcList, List<String> dstList,  List<Long> offset,
                         List<Long> length, List<String> concat, List<Long> concatnum, List<Long> blocksize) {
        super(action);
        this.srcList = new ArrayList<>();
        this.dstList = new ArrayList<>();
        this.offsetList = new ArrayList<>();
        this.lengthList = new ArrayList<>();
        this.concatList = new ArrayList<>();
        this.concatnum = new ArrayList<>();
        this.blocksize = new ArrayList<>();
        for (String s : srcList)
            this.srcList.add(s);
        for (String s : dstList)
            this.dstList.add(s);
        for (Long l : offset)
            this.offsetList.add(l);
        for (Long l : length)
            this.lengthList.add(l);
        for (String s : concat)
            this.concatList.add(s);
        for (Long l : concatnum)
            this.concatnum.add(l);
        for (Long l : blocksize)
            this.blocksize.add(l);
    }
}
