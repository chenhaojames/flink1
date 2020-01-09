package xia.v.lan.AllCountSample;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.HashMap;
import java.util.Random;

/**
 * @author chenhao
 * @description <p>
 * created by chenhao 2020/1/8 17:14
 *
 * 示例的实现如表4所示。首先，在该实现中，我们首先实现了一个模拟的数据源，它继承自 RichParallelSourceFunction，它是可以有多个实例的 SourceFunction 的接口。它有两个方法需要实现，一个是 Run 方法，Flink 在运行时对 Source 会直接调用该方法，该方法需要不断的输出数据，从而形成初始的流。在 Run 方法的实现中，我们随机的产生商品类别和交易量的记录，然后通过 ctx#collect 方法进行发送。另一个方法是 Cancel 方法，当 Flink 需要 Cancel Source Task 的时候会调用该方法，我们使用一个 Volatile 类型的变量来标记和控制执行的状态。
 *
 * 然后，我们在 Main 方法中就可以开始图的构建。我们首先创建了一个 StreamExecutioniEnviroment 对象。创建对象调用的 getExecutionEnvironment 方法会自动判断所处的环境，从而创建合适的对象。例如，如果我们在 IDE 中直接右键运行，则会创建 LocalStreamExecutionEnvironment 对象；如果是在一个实际的环境中，则会创建 RemoteStreamExecutionEnvironment 对象。
 *
 * 基于 Environment 对象，我们首先创建了一个 Source，从而得到初始的<商品类型，成交量>流。然后，为了统计每种类别的成交量，我们使用 KeyBy 按 Tuple 的第 1 个字段（即商品类型）对输入流进行分组，并对每一个 Key 对应的记录的第 2 个字段（即成交量）进行求合。在底层，Sum 算子内部会使用 State 来维护每个Key（即商品类型）对应的成交量之和。当有新记录到达时，Sum 算子内部会更新所维护的成交量之和，并输出一条<商品类型，更新后的成交量>记录。
 *
 * 如果只统计各个类型的成交量，则程序可以到此为止，我们可以直接在 Sum 后添加一个 Sink 算子对不断更新的各类型成交量进行输出。但是，我们还需要统计所有类型的总成交量。为了做到这一点，我们需要将所有记录输出到同一个计算节点的实例上。我们可以通过 KeyBy 并且对所有记录返回同一个 Key，将所有记录分到同一个组中，从而可以全部发送到同一个实例上。
 *
 * 然后，我们使用 Fold 方法来在算子中维护每种类型商品的成交量。注意虽然目前 Fold 方法已经被标记为 Deprecated，但是在 DataStream API 中暂时还没有能替代它的其它操作，所以我们仍然使用 Fold 方法。这一方法接收一个初始值，然后当后续流中每条记录到达的时候，算子会调用所传递的 FoldFunction 对初始值进行更新，并发送更新后的值。我们使用一个 HashMap 来对各个类别的当前成交量进行维护，当有一条新的<商品类别，成交量>到达时，我们就更新该 HashMap。这样在 Sink 中，我们收到的是最新的商品类别和成交量的 HashMap，我们可以依赖这个值来输出各个商品的成交量和总的成交量。
 *
 */
public class GroupedProcessingTimeWindowSample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Tuple2<String, Integer>> dataStreamSource = env.addSource(new DataSource());
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = dataStreamSource.keyBy(0);
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(20), Time.seconds(10));
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windowedStream.sum(1);
        sum.keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return "";
            }
        }).fold(new HashMap<String, Integer>(), new FoldFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> fold(HashMap<String, Integer> stringIntegerHashMap, Tuple2<String, Integer> o) throws Exception {
                stringIntegerHashMap.put(o.f0,o.f1);
                return stringIntegerHashMap;
            }
        }).addSink(new SinkFunction<HashMap<String, Integer>>() {
            @Override
            public void invoke(HashMap<String, Integer> value, Context context) throws Exception {
                System.out.println(value);
                System.out.println(value.values().stream().mapToInt(v -> v).sum());
            }
        });
        env.execute("GroupedProcessingTimeWindowSample");
    }

    private static class DataSource extends RichParallelSourceFunction<Tuple2<String,Integer>>{

        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            Random rd = new Random();
            while(isRunning){
                Thread.sleep((getRuntimeContext().getIndexOfThisSubtask() + 1)*1000*5);
                String key = "类别" + (char)('A' + rd.nextInt(3));
                int value = rd.nextInt(10) + 1;
                System.out.println(String.format("Emits\t(%s, %d)", key, value));
                ctx.collect(new Tuple2<>(key,value));
            }
        }

        @Override
        public void cancel() {
            this.isRunning = false;
        }
    }
}
