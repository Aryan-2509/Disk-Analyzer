package org.example;

public class Col implements Comparable<Col>{
    String name;
    int storageSize;
    int freeStorageSize;
    double freePercent;

    Col(String name,int s,int f)
    {
        this.name = name;
        storageSize = s;
        freeStorageSize = f;
        freePercent = ((double)f/(double) s)*100;
    }
    Col() {}

    public int compareTo(Col that) {

        if(this.freePercent < that.freePercent)
        {
            return 1;
        }
        return -1;
    }
}