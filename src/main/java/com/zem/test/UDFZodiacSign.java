package com.zem.test;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.SimpleDateFormat;

public class UDFZodiacSign extends UDF {
    private SimpleDateFormat df;
    
    public UDFZodiacSign() {
        df = new SimpleDateFormat("MM-dd-yyyy");
    }
}
