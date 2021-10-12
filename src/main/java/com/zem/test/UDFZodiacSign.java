package com.zem.test;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.SimpleDateFormat;
import java.util.Date;

public class UDFZodiacSign extends UDF {
    private SimpleDateFormat df;

    public UDFZodiacSign() {
        df = new SimpleDateFormat("MM-dd-yyyy");
    }

    public String evaluate(Date bday){
        return this.evaluate(bday.getMonth(),bday.getDay());
    }

    public String evaluate(Integer month ,Integer day){
        if(month <= 6){
            return "type A";
        }else {
            return "type B";
        }
    }
}
