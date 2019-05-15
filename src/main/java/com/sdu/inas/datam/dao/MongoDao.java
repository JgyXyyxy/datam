package com.sdu.inas.datam.dao;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.sdu.inas.datam.bean.Doc;
import com.sdu.inas.datam.bean.Person;
import com.sdu.inas.datam.util.CommonUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import javax.validation.constraints.Null;
import java.util.ArrayList;
import java.util.List;


@Repository
public class MongoDao {

    @Autowired
    MongoTemplate mongoTemplate;

    @SuppressWarnings("unchecked")
    public ArrayList<Doc> getAllDoc() {
        ArrayList<Doc> docs = new ArrayList<>();
        DBCollection dbCollection = mongoTemplate.getCollection("event");
        DBCursor cursor = dbCollection.find();
        ArrayList<String> pName = new ArrayList<>();
        ArrayList<String> sName = new ArrayList<>();
        int i = 0;
        while (cursor.hasNext()) {
            DBObject object = cursor.next();
            pName.clear();
            sName.clear();
            if (object.get("pName") instanceof java.util.List) {
                pName = (ArrayList<String>) object.get("pName");
            } else {
                String p = (String) object.get("pName");
                pName.add(p);
            }
            if (object.get("sName") instanceof java.util.List) {
                sName = (ArrayList<String>) object.get("sName");
            } else {
                String s = (String) object.get("sName");
                sName.add(s);
            }
            i++;
            String details = (String) object.get("event");
            String date = (String) object.get("date");
            String t = CommonUtil.fillTime(date.replace("/", "-"));
            System.out.println("have got: " + String.valueOf(i) + "  " + details);
            ArrayList<String> p = new ArrayList<>(pName);
            ArrayList<String> s = new ArrayList<>(sName);
            Doc doc = new Doc(t, p, s, details);
            docs.add(doc);
        }

        return docs;

    }

    public String getBasicInfo(String pName) {

        List<Person> people = mongoTemplate.find(new Query(Criteria.where("pName").is(pName)), Person.class);
        if (people.size()!=0){
            Person person = people.get(0);
            if (person.getPBaseInfo()==null){
                return "";
            }
            return person.getPBaseInfo();
        }else {
            return "";
        }
    }

    public ArrayList<Person> getAllPerson(){
        ArrayList<Person> people = new ArrayList<>();
        DBCollection dbCollection = mongoTemplate.getCollection("person");
        DBCursor cursor = dbCollection.find();

        int i = 0;
        while (cursor.hasNext()) {
            DBObject object = cursor.next();
            i++;
            String base = (String) object.get("pBaseInfo");
            String name = (String) object.get("pName");
            System.out.println("have got: " + String.valueOf(i) + "  " + name+"  "+ base);
            Person person = new Person(name, null, base);
            people.add(person);
        }

        return people;
    }

}
