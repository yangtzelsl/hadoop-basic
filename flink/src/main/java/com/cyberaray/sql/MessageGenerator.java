package com.cyberaray.sql;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * @description 电商场景实战之实时PV和UV曲线的数据模拟
 * @author: ZhiWen
 * @create: 2020-01-19 10:56
 **/
public class MessageGenerator {

    /**
     * 位置
     */
    private static String[] position = new String[]{"北京", "天津", "抚州"};


    /**
     * 网络方式
     */
    private static String[] networksUse = new String[]{"4G", "2G", "WIFI", "5G"};

    /**
     * 来源方式
     */
    private static String[] sources = new String[]{"直接输入", "百度跳转", "360搜索跳转", "必应跳转"};

    /**
     * 浏览器
     */
    private static String[] brower = new String[]{"火狐浏览器", "qq浏览器", "360浏览器", "谷歌浏览器"};

    /**
     * 客户端信息
     */
    private static String[] clientInfo = new String[]{"Android", "IPhone OS", "None", "Windows Phone", "Mac"};

    /**
     * 客户端IP
     */
    private static String[] clientIp = new String[]{"172.24.103.101", "172.24.103.102", "172.24.103.103", "172.24.103.104", "172.24.103.1", "172.24.103.2", "172.24.103.3", "172.24.103.4", "172.24.103.5", "172.24.103.6"};

    /**
     * 埋点链路
     */
    private static String[] gpm = new String[]{"http://172.24.103.102:7088/controlmanage/platformOverView", "http://172.24.103.103:7088/datamanagement/metadata", "http://172.24.103.104:7088/projectmanage/projectList"};


    public static void main(String[] args) {

        //配置信息
        Properties props = new Properties();
        //kafka服务器地址
        props.put("bootstrap.servers", "172.24.103.8:9092");
        //设置数据key和value的序列化处理类
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        //创建生产者实例
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);


        //每 1 秒请模拟求一次
        Random random = new Random();
        while (true) {

            Message01 message01 = new Message01();
            message01.setAccount_id(UUID.randomUUID().toString());
            message01.setClient_ip("172.24.103." + random.nextInt(255));
            message01.setClient_info(clientInfo[random.nextInt(clientInfo.length)]);
            message01.setAction(sources[random.nextInt(sources.length)]);
            message01.setGpm(gpm[random.nextInt(gpm.length)]);
            //message01.setC_time(System.currentTimeMillis() / 1000);
            message01.setUdata("json格式扩展信息");
            message01.setPosition(position[random.nextInt(position.length)]);
            message01.setNetwork(networksUse[random.nextInt(networksUse.length)]);
            //message01.setP_dt(DateUtils.getCurrentDateOfPattern("yyyy-MM-dd"));

            String json = JSONObject.toJSONString(message01);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            ProducerRecord record = new ProducerRecord<String, String>("topic_uv", json);
            //发送记录
            producer.send(record);
            System.out.println(json);
        }
    }
}


class Message01 {

    /*
        account_id                        VARCHAR,--用户ID。
        client_ip                         VARCHAR,--客户端IP。
        client_info                       VARCHAR,--设备机型信息。
        platform                          VARCHAR,--系统版本信息。
        imei                              VARCHAR,--设备唯一标识。
        `version`                         VARCHAR,--版本号。
        `action`                          VARCHAR,--页面跳转描述。
        gpm                               VARCHAR,--埋点链路。
        c_time                            VARCHAR,--请求时间。
        target_type                       VARCHAR,--目标类型。
        target_id                         VARCHAR,--目标ID。
        udata                             VARCHAR,--扩展信息，JSON格式。
        session_id                        VARCHAR,--会话ID。
        product_id_chain                  VARCHAR,--商品ID串。
        cart_product_id_chain             VARCHAR,--加购商品ID。
        tag                               VARCHAR,--特殊标记。
        `position`                        VARCHAR,--位置信息。
        network                           VARCHAR,--网络使用情况。
        p_dt                              VARCHAR,--时间分区天。
        p_platform                        VARCHAR --系统版本信息。
    */

    private String account_id;
    private String client_ip;
    private String client_info;
    private String platform;
    private String imei;
    private String version;
    private String action;
    private String gpm;
    private String c_time;
    private String target_type;
    private String target_id;
    private String udata;
    private String session_id;
    private String product_id_chain;
    private String cart_product_id_chain;
    private String tag;
    private String position;
    private String network;
    private String p_dt;
    private String p_platform;

    public Message01() {
    }

    @Override
    public String toString() {
        return "Message01{" +
                "account_id='" + account_id + '\'' +
                ", client_ip='" + client_ip + '\'' +
                ", client_info='" + client_info + '\'' +
                ", platform='" + platform + '\'' +
                ", imei='" + imei + '\'' +
                ", version='" + version + '\'' +
                ", action='" + action + '\'' +
                ", gpm='" + gpm + '\'' +
                ", c_time='" + c_time + '\'' +
                ", target_type='" + target_type + '\'' +
                ", target_id='" + target_id + '\'' +
                ", udata='" + udata + '\'' +
                ", session_id='" + session_id + '\'' +
                ", product_id_chain='" + product_id_chain + '\'' +
                ", cart_product_id_chain='" + cart_product_id_chain + '\'' +
                ", tag='" + tag + '\'' +
                ", position='" + position + '\'' +
                ", network='" + network + '\'' +
                ", p_dt='" + p_dt + '\'' +
                ", p_platform='" + p_platform + '\'' +
                '}';
    }

    public String getAccount_id() {
        return account_id;
    }

    public void setAccount_id(String account_id) {
        this.account_id = account_id;
    }

    public String getClient_ip() {
        return client_ip;
    }

    public void setClient_ip(String client_ip) {
        this.client_ip = client_ip;
    }

    public String getClient_info() {
        return client_info;
    }

    public void setClient_info(String client_info) {
        this.client_info = client_info;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getGpm() {
        return gpm;
    }

    public void setGpm(String gpm) {
        this.gpm = gpm;
    }

    public String getC_time() {
        return c_time;
    }

    public void setC_time(String c_time) {
        this.c_time = c_time;
    }

    public String getTarget_type() {
        return target_type;
    }

    public void setTarget_type(String target_type) {
        this.target_type = target_type;
    }

    public String getTarget_id() {
        return target_id;
    }

    public void setTarget_id(String target_id) {
        this.target_id = target_id;
    }

    public String getUdata() {
        return udata;
    }

    public void setUdata(String udata) {
        this.udata = udata;
    }

    public String getSession_id() {
        return session_id;
    }

    public void setSession_id(String session_id) {
        this.session_id = session_id;
    }

    public String getProduct_id_chain() {
        return product_id_chain;
    }

    public void setProduct_id_chain(String product_id_chain) {
        this.product_id_chain = product_id_chain;
    }

    public String getCart_product_id_chain() {
        return cart_product_id_chain;
    }

    public void setCart_product_id_chain(String cart_product_id_chain) {
        this.cart_product_id_chain = cart_product_id_chain;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public String getNetwork() {
        return network;
    }

    public void setNetwork(String network) {
        this.network = network;
    }

    public String getP_dt() {
        return p_dt;
    }

    public void setP_dt(String p_dt) {
        this.p_dt = p_dt;
    }

    public String getP_platform() {
        return p_platform;
    }

    public void setP_platform(String p_platform) {
        this.p_platform = p_platform;
    }
}
