package com.sdu.inas.datam.service;


import com.sdu.inas.datam.bean.Doc;
import com.sdu.inas.datam.bean.Event;
import com.sdu.inas.datam.bean.HbaseModel;
import com.sdu.inas.datam.bean.RealEntity;
import com.sdu.inas.datam.dao.EventRepository;
import com.sdu.inas.datam.dao.HbaseDao;
import com.sdu.inas.datam.dao.MongoDao;
import com.sdu.inas.datam.util.CommonUtil;
import com.sdu.inas.datam.util.HbaseModelUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;


@Service
public class Mongo2Hbase {

    @Autowired
    MongoDao mongoDao;

    @Autowired
    HbaseDao hbaseDao;

    @Autowired
    EventRepository eventRepository;

    public  void transData(){
        ArrayList<Doc> allDoc = mongoDao.getAllDoc();
        for (Doc doc:allDoc){
            List<String> pName = doc.getPName();
            for (String s:pName){
                List<RealEntity> enByPrefix = getEnByPrefix(s);
                if (enByPrefix.size()==0){
                    String objectId = s + CommonUtil.genRandomNum();
                    String realName = s +" ";
                    insertRealName(realName, objectId);
                    String  eventId = CommonUtil.getUUID();
                    insertEvent(objectId,new Event(eventId,objectId,"2050-01-01","",realName,""));
                    Event event = new Event(CommonUtil.getUUID(), objectId, doc.getDate(), doc.getSName().get(0), doc.getDetails(), "");
                    insertEvent(objectId,event);
                }else {
                    RealEntity realEntity = enByPrefix.get(0);
                    Event event = new Event(CommonUtil.getUUID(), realEntity.getObjectId(), doc.getDate(), doc.getSName().get(0), doc.getDetails(), "");
                    insertEvent(realEntity.getObjectId(),event);
                }
            }
        }
    }


    private List<RealEntity> getEnByPrefix(String prefix){
        List<RealEntity> entityList = null;
        try {
            entityList = findEntitiesByPrefix(prefix);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return entityList;
    }


    private void insertEvent(String objectId, Event event) {
        hbaseDao.insertData(HbaseModelUtil.BASIC_TABLE,objectId,HbaseModelUtil.BASIC_EVENT,event.getTs(),event.getEventId(),null);
        hbaseDao.insertData(HbaseModelUtil.EVENTS_TABLE,event.getEventId(),HbaseModelUtil.EVENTS_PARAMS,"ts",event.getTs(),null);
        hbaseDao.insertData(HbaseModelUtil.EVENTS_TABLE,event.getEventId(),HbaseModelUtil.EVENTS_PARAMS,"site",event.getSite(),null);
        hbaseDao.insertData(HbaseModelUtil.EVENTS_TABLE,event.getEventId(),HbaseModelUtil.EVENTS_PARAMS,"details",event.getDetails(),null);
        hbaseDao.insertData(HbaseModelUtil.EVENTS_TABLE,event.getEventId(),HbaseModelUtil.EVENTS_PARAMS,"affect",event.getAffect(),null);
        eventRepository.deleteEventByEventId(event.getEventId());
        eventRepository.save(event);
    }

    private void insertRealName(String realName, String objectId) {
        hbaseDao.insertData(HbaseModelUtil.BASIC_TABLE,objectId,HbaseModelUtil.BASIC_RAW,HbaseModelUtil.COLUMN1,realName,null);
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


}
