package com.bj58.risktask.stream.window;

import com.bj58.risktask.stream.Event;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;

public class UvCountByWindow extends ProcessWindowFunction<Event, String, String, TimeWindow> {

    private String name;

    public UvCountByWindow() {}

    public UvCountByWindow(String name) {
        this.name = name;
    }

    @Override
    public void process(String key, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
        HashSet<String> userSet = new HashSet<>();
        // 遍历所有数据，放到Set里去重
        for (Event event : elements) {
            userSet.add(event.user);
        }
        // 结合窗口信息，包装输出内容
        Long start = context.window().getStart();
        Long end = context.window().getEnd();
        out.collect("key:   " + key + " ~   " + "窗口:    " + new Timestamp(start) + "    ~   " + new Timestamp(end)
                + " 的独立访客数量是：" + userSet.size() + " ==========> " + name);
    }
}
