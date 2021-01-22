package cn.ctyun.oos.bssAdapter.orderbill.sender;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.JSONArray;
import org.json.JSONObject;

public class OrderMessage {
    private List<JSONObject> orders;
    private AtomicInteger retryTimes;

    public OrderMessage(List<JSONObject> orders, int retryTimes) {
        this.orders = orders;
        this.retryTimes = new AtomicInteger(retryTimes);
    }

    public List getOrders() {
        return orders;
    }

    public int getRetryTimes() {
        return retryTimes.get();
    }

    public void decrementTimes() {
        retryTimes.decrementAndGet();
    }
}
