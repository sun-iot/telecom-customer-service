package com.ci123.product;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: telecom-customer-service
 * Package: com.ci123.model
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/7 16:50
 */
public class ProductLog {
    // 存放联系人电话和姓名的映射
    public Map<String, String> phonePerson = null;
    // 存放联系人电话号码
    public List<String> phoneList = null;

    public String base = "abcdefghijklmnopqrstuvwxyz0123456789";
    private String firstName = "赵钱孙李周吴郑王冯陈褚卫蒋沈韩杨朱秦尤许何吕施张孔曹严华金魏陶姜戚谢邹喻柏水窦章云苏潘葛奚" +
            "范彭郎鲁韦昌马苗凤花方俞任袁柳酆鲍史唐费廉岑薛雷贺倪汤滕殷罗毕郝邬安常乐于时傅皮卞齐康伍余元卜顾孟平黄和穆萧尹姚邵湛汪" +
            "祁毛禹狄米贝明臧计伏成戴谈宋茅庞熊纪舒屈项祝董梁杜阮蓝闵席季麻强贾路娄危江童颜郭梅盛林刁钟徐邱骆高夏蔡田樊胡凌霍虞万支" +
            "柯咎管卢莫经房裘缪干解应宗宣丁贲邓郁单杭洪包诸左石崔吉钮龚程嵇邢滑裴陆荣翁荀羊於惠甄魏加封芮羿储靳汲邴糜松井段富巫乌焦" +
            "巴弓牧隗山谷车侯宓蓬全郗班仰秋仲伊宫宁仇栾暴甘钭厉戎祖武符刘姜詹束龙叶幸司韶郜黎蓟薄印宿白怀蒲台从鄂索咸籍赖卓蔺屠蒙池" +
            "乔阴郁胥能苍双闻莘党翟谭贡劳逄姬申扶堵冉宰郦雍却璩桑桂濮牛寿通边扈燕冀郏浦尚农温别庄晏柴瞿阎充慕连茹习宦艾鱼容向古易慎" +
            "戈廖庚终暨居衡步都耿满弘匡国文寇广禄阙东殴殳沃利蔚越夔隆师巩厍聂晁勾敖融冷訾辛阚那简饶空曾毋沙乜养鞠须丰巢关蒯相查后江" +
            "红游竺权逯盖益桓公万俟司马上官欧阳夏侯诸葛闻人东方赫连皇甫尉迟公羊澹台公冶宗政濮阳淳于仲孙太叔申屠公孙乐正轩辕令狐钟离" +
            "闾丘长孙慕容鲜于宇文司徒司空亓官司寇仉督子车颛孙端木巫马公西漆雕乐正壤驷公良拓拔夹谷宰父谷粱晋楚阎法汝鄢涂钦段干百里东" +
            "郭南门呼延归海羊舌微生岳帅缑亢况后有琴梁丘左丘东门西门商牟佘佴伯赏南宫墨哈谯笪年爱阳佟第五言福百家姓续";
    private static String girl = "秀娟英华慧巧美娜静淑惠珠翠雅芝玉萍红娥玲芬芳燕彩春菊兰凤洁梅琳素云莲真环雪荣爱妹霞香月莺媛艳瑞凡" +
            "佳嘉琼勤珍贞莉桂娣叶璧璐娅琦晶妍茜秋珊莎锦黛青倩婷姣婉娴瑾颖露瑶怡婵雁蓓纨仪荷丹蓉眉君琴蕊薇菁梦岚苑婕馨瑗琰韵融园艺咏" +
            "卿聪澜纯毓悦昭冰爽琬茗羽希宁欣飘育滢馥筠柔竹霭凝晓欢霄枫芸菲寒伊亚宜可姬舒影荔枝思丽 ";
    private static String boy = "伟刚勇毅俊峰强军平保东文辉力明永健世广志义兴良海山仁波宁贵福生龙元全国胜学祥才发武新利清飞彬富顺信" +
            "子杰涛昌成康星光天达安岩中茂进林有坚和彪博诚先敬震振壮会思群豪心邦承乐绍功松善厚庆磊民友裕河哲江超浩亮政谦亨奇固之轮翰" +
            "朗伯宏言若鸣朋斌梁栋维启克伦翔旭鹏泽晨辰士以建家致树炎德行时泰盛雄琛钧冠策腾楠榕风航弘";


    public void init() {
        phonePerson = new HashMap<String, String>();
        phoneList = new ArrayList<String>();

        for (int i = 0; i <= 100; i++) {
            String tel = getTel();
            String name = getName();
            phoneList.add(tel);
            phonePerson.put(tel, name);
        }
    }

    public int getNum(int start, int end) {
        return (int) (Math.random() * (end - start) + start);
    }

    /**
     * 返回手机号码
     */
    private String[] telFirst = "134,135,136,137,138,139,150,151,152,157,158,159,130,131,132,155,156,133,153".split(",");

    private String getTel() {
        int index = getNum(0, telFirst.length - 1);
        String first = telFirst[index];
        String second = String.valueOf(getNum(1, 888) + 10000).substring(1);
        String third = String.valueOf(getNum(1, 9100) + 10000).substring(1);
        return first + second + third;
    }

    /**
     * 返回中文姓名
     */
    private String name_sex = "";

    private String getName() {
        int index = getNum(0, firstName.length() - 1);
        String first = firstName.substring(index, index + 1);
        int sex = getNum(0, 1);
        String str = boy;
        int length = boy.length();
        if (sex == 0) {
            str = girl;
            length = girl.length();
            name_sex = "女";
        } else {
            name_sex = "男";
        }
        index = getNum(0, length - 1);
        String second = str.substring(index, index + 1);
        int hasThird = getNum(0, 1);
        String third = "";
        if (hasThird == 1) {
            index = getNum(0, length - 1);
            third = str.substring(index, index + 1);
        }
        return first + second + third;

    }

    //随机生成通话建立时间
    private String randomDate(String startDate, String endDate) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date start = simpleDateFormat.parse(startDate);
            Date end = simpleDateFormat.parse(endDate);

            if (start.getTime() > end.getTime()) return null;

            long resultTime = start.getTime() + (long) (Math.random() * (end.getTime() - start.getTime()));
            return resultTime + "";
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    // 拼接日志
    private String productLog() {
        int call1Index = new Random().nextInt(phoneList.size());
        int call2Index = -1;

        String call1 = phoneList.get(call1Index);
        String call1_name = phonePerson.get(call1);

        String call2 = null;
        String call2_name = null;
        while (true) {
            call2Index = new Random().nextInt(phoneList.size());
            call2 = phoneList.get(call2Index);
            call2_name = phonePerson.get(call2);
            if (!call1.equals(call2)) break;
        }
        // 随机生成通话时长(30分钟内_0600)
        int duration = new Random().nextInt(60 * 30) + 1;

        // 格式化通话时间，使位数一致
        String durationString = new DecimalFormat("0000").format(duration);
        // 通话建立时间:yyyy-MM-dd,月份：0~11，天：1~31
        String randomDate = randomDate("2017-01-01", "2018-01-01");
        String dateString = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Long.parseLong(randomDate));
        long timestamp = LocalDateTime.from(LocalDateTime.parse(dateString,
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
                .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        // 拼接log日志
        StringBuilder logBuilder = new StringBuilder();

        logBuilder.append(call1).append(",")
                .append(call1_name).append(",")
                .append(call2).append(",")
                .append(call2_name).append(",")
                .append(dateString).append(",")
                .append(timestamp).append(",")
                .append(durationString);
        System.out.println(logBuilder);
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return logBuilder.toString();
    }

    /**
     * 将产生的日志写入到本地文件calllog中
     * @param filePath
     * @param product
     */
    public void writeLog(String filePath, ProductLog product) {
        OutputStreamWriter outputStreamWriter = null;
        try {
            outputStreamWriter = new OutputStreamWriter(new FileOutputStream(filePath, true), "UTF-8");
            while (true) {
                String log = product.productLog();
                outputStreamWriter.write(log + "\n");
                outputStreamWriter.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                assert outputStreamWriter != null;
                outputStreamWriter.flush();
                outputStreamWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


}
