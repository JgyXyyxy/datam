package com.sdu.inas.datam.service;


import com.sdu.inas.datam.bean.*;
import com.sdu.inas.datam.dao.EventRepository;
import com.sdu.inas.datam.dao.HbaseDao;
import com.sdu.inas.datam.dao.MongoDao;
import com.sdu.inas.datam.util.CommonUtil;
import com.sdu.inas.datam.util.HbaseModelUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.mapred.IFile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;


@Service
public class Mongo2Hbase {

    @Autowired
    MongoDao mongoDao;

    @Autowired
    HbaseDao hbaseDao;

    @Autowired
    EventRepository eventRepository;

    public void transData() throws IOException {


        ArrayList<Doc> allDoc = mongoDao.getAllDoc();
        int i = 0;
        for (Doc doc : allDoc) {
            i++;
            if (i < 9665) {

            } else {
                List<String> pName = doc.getPName();
                System.out.println("---------------------------------------------------");
                System.out.println("开始插入第 " + i + " 个事件");
                for (String s : pName) {
                    System.out.println("实体名称： " + s);
                    String realName = s + " ";
                    String basicInfo = mongoDao.getBasicInfo(s);
                    if (basicInfo.length() < 20) {
                        realName = s + " " + basicInfo;
                    }
                    List<String> objs = getObIdByPrefix(s);
                    List<String> sName = doc.getSName();
                    StringBuilder site = new StringBuilder();
                    for (String sn : sName) {
                        site.append(sn);
                        site.append(" ");
                    }
                    if (objs.size() == 0) {
                        System.out.println("库内不存在该实体，正在创建");
                        String objectId = s + CommonUtil.genRandomNum();

                        insertRealName(realName, objectId);
                        String eventId = CommonUtil.getUUID();
                        insertEvent(objectId, new Event(eventId, objectId, "2050-01-01", "", realName, ""));
                        Event event = new Event(CommonUtil.getUUID(), objectId, doc.getDate(), site.toString(), doc.getDetails(), "");
                        insertEvent(objectId, event);
                        System.out.println("插入第 " + i + " 个事件中 " + s + " 成功");
                    } else {
                        String objectId = getRightOne(objs, s);
                        if (objectId != null) {
                            Event event = new Event(CommonUtil.getUUID(), objectId, doc.getDate(), site.toString(), doc.getDetails(), "");
                            insertEvent(objectId, event);
                            System.out.println("插入第 " + i + " 个事件中 " + s + " 成功");
                        } else {
                            System.out.println("返回列表中未发现该实体，" + "插入第 " + i + " 个事件中 " + s + " 失败");
                        }
                    }
                }
            }

        }
    }


    public void transBaseInfo() {

        ArrayList<Person> allPerson = mongoDao.getAllPerson();
        int i = 0;
        for (Person person : allPerson) {
            i++;
            if (i < 1) {

            } else {
                String pName = person.getPName();
                List<String> obIdByPrefix = getObIdByPrefix(pName);
                if (obIdByPrefix.size() == 0) {
                    System.out.println("库内不存在该实体，正在创建");
                    String objectId = pName + CommonUtil.genRandomNum();
                    String realName = pName + " ";
                    String basicInfo = mongoDao.getBasicInfo(pName);
                    if (basicInfo.length() < 20) {
                        realName = pName + " " + basicInfo;
                    }
                    insertRealName(realName, objectId);
                    String eventId = CommonUtil.getUUID();
                    insertEvent(objectId, new Event(eventId, objectId, "2050-01-01", "", realName, ""));
                    addRawText(person.getPBaseInfo(), objectId);
                    System.out.println("插入第 " + i + " 个实体 " + objectId + " 的原始信息成功");

                }
                String objectId = getRightOne(obIdByPrefix, pName);
                if (objectId != null) {
                    addRawText(person.getPBaseInfo(), objectId);
                    System.out.println("插入第 " + i + " 个实体 " + objectId + " 的原始信息成功");
                } else {
                    System.out.println("返回列表中未发现该实体，" + "插入第 " + i + " 个实体 " + pName + " 失败");
                }
            }

        }
    }

    private String getRightOne(List<String> objs, String s) {
        for (String obj : objs) {
            if (obj.compareTo(s) == 8) {
                return obj;
            }
        }

        return null;
    }


    public void getBasic(String pName) {
        String info = mongoDao.getBasicInfo(pName);
        System.out.println(info);
    }

    public static void main(String[] args) {
        String s1 = "1998/11/3";
        String s2 = "曹操";
        String replace = s1.replace("/", "-");
        System.out.println(replace);
        System.out.println(s1.compareTo(s2));
    }


    private List<RealEntity> getEnByPrefix(String prefix) {
        List<RealEntity> entityList = null;
        try {
            entityList = findEntitiesByPrefix(prefix);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return entityList;
    }

    private List<String> getObIdByPrefix(String prefix) {
        List<String> objs = null;
        try {
            objs = findObIdByPrefix(prefix);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return objs;
    }


    private void insertEvent(String objectId, Event event) {
        hbaseDao.insertData(HbaseModelUtil.BASIC_TABLE, objectId, HbaseModelUtil.BASIC_EVENT, event.getTs(), event.getEventId(), null);
        hbaseDao.insertData(HbaseModelUtil.EVENTS_TABLE, event.getEventId(), HbaseModelUtil.EVENTS_PARAMS, "ts", event.getTs(), null);
        hbaseDao.insertData(HbaseModelUtil.EVENTS_TABLE, event.getEventId(), HbaseModelUtil.EVENTS_PARAMS, "site", event.getSite(), null);
        hbaseDao.insertData(HbaseModelUtil.EVENTS_TABLE, event.getEventId(), HbaseModelUtil.EVENTS_PARAMS, "details", event.getDetails(), null);
        hbaseDao.insertData(HbaseModelUtil.EVENTS_TABLE, event.getEventId(), HbaseModelUtil.EVENTS_PARAMS, "affect", event.getAffect(), null);
        eventRepository.deleteEventByEventId(event.getEventId());
        eventRepository.save(event);
    }

    private void insertRealName(String realName, String objectId) {
        hbaseDao.insertData(HbaseModelUtil.BASIC_TABLE, objectId, HbaseModelUtil.BASIC_RAW, HbaseModelUtil.COLUMN1, realName, null);
    }

    private List<RealEntity> findEntitiesByPrefix(String prefix) throws Exception {
        List<Result> rets = hbaseDao.getDataWithSameBegining(HbaseModelUtil.BASIC_TABLE, prefix);
        Iterator<Result> iterator = rets.iterator();
        HashMap<String, RealEntity> entityHashMap = new HashMap<>();
        while (iterator.hasNext()) {
            Result ret = iterator.next();
            for (KeyValue kv : ret.list()) {
                HbaseModel hbaseModel = HbaseModelUtil.kvToHbaseModel(kv);
                if (entityHashMap.containsKey(hbaseModel.getRow())) {
                    RealEntity entity = entityHashMap.get(hbaseModel.getRow());
                    RealEntity realEntity = packageModel(entity, hbaseModel);
                    entityHashMap.remove(hbaseModel.getRow());
                    entityHashMap.put(hbaseModel.getRow(), realEntity);

                } else {
                    RealEntity entity = new RealEntity();
                    entity.setObjectId(hbaseModel.getRow());
                    RealEntity realEntity = packageModel(entity, hbaseModel);
                    entityHashMap.put(hbaseModel.getRow(), realEntity);
                }
            }
        }

        ArrayList<RealEntity> entities = new ArrayList<>();
        Iterator<String> iterator1 = entityHashMap.keySet().iterator();
        while (iterator1.hasNext()) {
            String s = iterator1.next();
            RealEntity entity = entityHashMap.get(s);
            entities.add(entity);
        }
        return entities;
    }

    private List<String> findObIdByPrefix(String prefix) {
        List<Result> rets = hbaseDao.getDataWithSameBegining(HbaseModelUtil.BASIC_TABLE, prefix);
        Iterator<Result> iterator = rets.iterator();
        ArrayList<String> objs = new ArrayList<>();
        while (iterator.hasNext()) {
            Result ret = iterator.next();
            for (KeyValue kv : ret.list()) {
                HbaseModel hbaseModel = HbaseModelUtil.kvToHbaseModel(kv);
                if (objs.contains(hbaseModel.getRow())) {

                } else {
                    objs.add(hbaseModel.getRow());
                }
            }
        }

        return objs;
    }

    private RealEntity packageModel(RealEntity realEntity, HbaseModel hbaseModel) {
        System.out.println(hbaseModel.getFamilyName());
        ArrayList<Event> eventList = realEntity.getEvents();
        if (HbaseModelUtil.BASIC_EVENT.equals(hbaseModel.getFamilyName())) {
            Event event = new Event();
            String eventId = hbaseModel.getValue();
            Result ret = hbaseDao.getDataFromRowkey(HbaseModelUtil.EVENTS_TABLE, eventId);
            for (KeyValue kv : ret.list()) {
                HbaseModel model = HbaseModelUtil.kvToHbaseModel(kv);
                String value = model.getValue();
                switch (model.getQualifier()) {
                    case "ts":
                        event.setTs(value);
                        break;
                    case "site":
                        event.setSite(value);
                        break;
                    case "details":
                        event.setDetails(value);
                        break;
                    default:
                        event.setAffect(value);
                }
                event.setEventId(eventId);
            }
            eventList.add(event);
        }
        if (HbaseModelUtil.BASIC_RAW.equals(hbaseModel.getFamilyName())) {
            if (hbaseModel.getQualifier().equals(HbaseModelUtil.COLUMN2)) {
                realEntity.setRawInfo(hbaseModel.getValue());
            } else if (hbaseModel.getQualifier().equals(HbaseModelUtil.COLUMN1)) {
                realEntity.setRealName(hbaseModel.getValue());
            } else {
                Map<String, String> params = realEntity.getParams();
                params.put(hbaseModel.getQualifier(), hbaseModel.getValue());
                realEntity.setParams(params);
            }

        }
        return realEntity;
    }


    public void addRawText(String rawText, String objectId) {
        hbaseDao.insertData(HbaseModelUtil.BASIC_TABLE, objectId, HbaseModelUtil.BASIC_RAW, HbaseModelUtil.RAW_TEXT, rawText, null);

    }


}
