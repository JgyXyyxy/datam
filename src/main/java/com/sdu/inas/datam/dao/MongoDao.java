package com.sdu.inas.datam.dao;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.sdu.inas.datam.bean.Doc;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;


@Repository
public class MongoDao {

    @Autowired
    MongoTemplate mongoTemplate;

    @SuppressWarnings("unchecked")
    public ArrayList<Doc> getAllDoc(){
        ArrayList<Doc> docs = new ArrayList<>();
        DBCollection dbCollection = mongoTemplate.getCollection("event");
        DBCursor cursor = dbCollection.find();
        ArrayList<String> pName = new ArrayList<>();
        ArrayList<String> sName = new ArrayList<>();
        int i = 0;
        while (cursor.hasNext()){
            DBObject object = cursor.next();
            pName.clear();
            sName.clear();
            if (object.get("pName") instanceof java.util.List){
                pName = (ArrayList<String>) object.get("pName");
            }else {
                String p = (String) object.get("pName");
                pName.add(p);
            }
            if (object.get("sName") instanceof java.util.List){
                sName = (ArrayList<String>) object.get("sName");
            }else {
                String s = (String) object.get("sName");
                sName.add(s);
            }
            i++;
            String details = (String)object.get("details");
            String date = (String) object.get("date");
            System.out.println("have got: "+String.valueOf(i)+"  "+details);
            Doc doc = new Doc(date, pName, sName, details);
            docs.add(doc);
        }

        return docs;

    }
}
