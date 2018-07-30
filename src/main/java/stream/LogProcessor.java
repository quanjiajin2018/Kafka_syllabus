package stream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[] ,byte[]> {

    private ProcessorContext context;
    //初始化
    public void init(ProcessorContext context) {
       this.context = context;
    }

    //处理每一条都调用
    public void process(byte[] key, byte[] value) {
        String inputOri = new String(value);
        //如果包含 >>> ,则去除
        if(inputOri.contains(">>>")){
            inputOri = inputOri.split(">>>")[1];
        }
        context.forward(key , inputOri.getBytes());

    }
    //周期性调用
    public void punctuate(long timestamp) {

    }

    //关闭
    public void close() {

    }
}
