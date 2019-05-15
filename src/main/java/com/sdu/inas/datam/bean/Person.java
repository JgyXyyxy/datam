package com.sdu.inas.datam.bean;


import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class Person {

    String pName;
    List<String> Names = new ArrayList<>();
    String pBaseInfo;

    public Person() {
    }

    public Person(String pName, List<String> names, String pBasicInfo) {
        this.pName = pName;
        this.Names = names;
        this.pBaseInfo = pBasicInfo;
    }
}
