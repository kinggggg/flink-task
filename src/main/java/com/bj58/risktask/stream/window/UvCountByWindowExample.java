package com.bj58.risktask.stream.window;


import com.bj58.risktask.stream.ClickSource;
import com.bj58.risktask.stream.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;


public class UvCountByWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // 将数据全部发往同一分区，按窗口统计UV
//        SingleOutputStreamOperator<String> process = stream.keyBy(data -> true)
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .process(new UvCountByWindow());
//        SingleOutputStreamOperator<String> process5 = getProcess(stream, 5, "五秒窗口");
        SingleOutputStreamOperator<String> process10 = getProcess(stream, 10, "十秒窗口");

        // 结果输出
//        process5.print();
        process10.print();

        StreamingFileSink<String> fileSink = StreamingFileSink
                .<String>forRowFormat(new Path("./output"),
                        new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();

        // 结果写入文件
//        process5.addSink(fileSink);

        env.execute();
    }

    public static SingleOutputStreamOperator<String> getProcess(SingleOutputStreamOperator<Event> stream, long windowSize, String name) {
        SingleOutputStreamOperator<String> process = stream.keyBy(data -> data.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .process(new UvCountByWindow(name));

        return process;
    }

}

