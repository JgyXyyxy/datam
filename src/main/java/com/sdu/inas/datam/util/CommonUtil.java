package com.sdu.inas.datam.util;

import java.lang.reflect.Array;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by J on  17-10-23.
 */

public class CommonUtil {

    public static String genRandomNum() {
        int maxNum = 36;
        int i;
        int count = 0;
        char[] str = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K',
                'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W',
                'X', 'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
        StringBuffer pwd = new StringBuffer("");
        Random r = new Random();
        while (count < 8) {
            i = Math.abs(r.nextInt(maxNum));
            if (i >= 0 && i < str.length) {
                pwd.append(str[i]);
                count++;
            }
        }
        return pwd.toString();
    }

    public static int toDays(String date) {
        String[] strings = date.split("-");
        Calendar ca = Calendar.getInstance();
        if (strings.length == 3) {
            ca.setTime(new Date(Integer.valueOf(strings[0]), Integer.valueOf(strings[1]) - 1, Integer.valueOf(strings[2])));
        }

        if (strings.length == 2) {
            ca.setTime(new Date(Integer.valueOf(strings[0]), Integer.valueOf(strings[1]) - 1, 1));
        } else {
            ca.setTime(new Date(Integer.valueOf(strings[0]), 1, 1));
        }
        return Integer.valueOf(strings[0]) * 365 + ca.get(Calendar.DAY_OF_YEAR);
    }

    public static String getUUID() {
        UUID uuid = UUID.randomUUID();
        return uuid.toString();
    }

    public static String transTimeStr(String timeStr) throws ParseException {
        String regex1 = "\\d{1,4}[-|/|年]\\d{1,2}[-|/|月]\\d{1,2}[-|/|日]";
        String regex2 = "\\d{1,4}[-|/|年]\\d{1,2}[-|/|月]";
        String regex3 = "\\d{1,4}[-|/|年]";
        StringBuilder time = new StringBuilder();
        Matcher matcher1 = Pattern.compile(regex1).matcher(timeStr);
        if (matcher1.find()) {
            String s = matcher1.group();
            Date d1 = new SimpleDateFormat("yyyy年MM月dd日").parse(s);//定义起始日期
            SimpleDateFormat sdf0 = new SimpleDateFormat("yyyy");
            SimpleDateFormat sdf1 = new SimpleDateFormat("MM");
            SimpleDateFormat sdf2 = new SimpleDateFormat("dd");
            time.append(sdf0.format(d1)).append("-").append(sdf1.format(d1)).append("-").append(sdf2.format(d1));
        } else {
            Matcher matcher2 = Pattern.compile(regex2).matcher(timeStr);
            if (matcher2.find()) {
                String s = matcher2.group();
                Date d1 = new SimpleDateFormat("yyyy年MM月").parse(s);//定义起始日期
                SimpleDateFormat sdf0 = new SimpleDateFormat("yyyy");
                SimpleDateFormat sdf1 = new SimpleDateFormat("MM");
                time.append(sdf0.format(d1)).append("-").append(sdf1.format(d1));
            } else {
                Matcher matcher3 = Pattern.compile(regex3).matcher(timeStr);
                if (matcher3.find()) {
                    String s = matcher3.group();
                    Date d1 = new SimpleDateFormat("yyyy年").parse(s);//定义起始日期
                    SimpleDateFormat sdf0 = new SimpleDateFormat("yyyy");
                    time.append(sdf0.format(d1));
                } else {
                    time.append("Not Found");
                }
            }
        }
        return time.toString();
    }


    public static String fillTime (String s) {

        ArrayList<String> tt = new ArrayList<>();
        tt.add("年");
        tt.add("月");
        tt.add("日");

        String[] split = s.split("-");
        StringBuilder t = new StringBuilder();
        for (int i = 0; i < split.length; i++) {
            t.append(split[i]);
            t.append(tt.get(i));
        }
        try {
            return (transTimeStr(t.toString()));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return "0001-01-01";

    }

    public static void main(String[] args) {
        String s = fillTime("1996-8-2");
        System.out.println(s);
    }
}
