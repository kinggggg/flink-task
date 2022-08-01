package com.bj58.risktask.stream.print;

import com.bj58.risktask.stream.Event;
import com.bj58.risktask.stream.ClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liweibo04 <liweibo04@58.com>
 * Created on 2022-08-01
 */
public class SourceTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //有了自定义的source function，调用addSource方法
        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        stream.print("SourceCustom");

        env.execute();
    }
}
