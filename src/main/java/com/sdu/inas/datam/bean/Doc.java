package com.sdu.inas.datam.bean;

import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.ArrayList;
import java.util.List;

@Document(collection = "event")
@Data
public class Doc {

    String date;
    List<String> pName = new ArrayList<>();
    List<String> sName = new ArrayList<>();
    String details;

    public Doc(String date, List<String> pName, List<String> sName, String details) {
        this.date = date;
        this.pName = pName;
        this.sName = sName;
        this.details = details;
    }

    public Doc() {
    }
}
