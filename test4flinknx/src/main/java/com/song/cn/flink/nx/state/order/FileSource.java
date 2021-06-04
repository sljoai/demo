package com.song.cn.flink.nx.state.order;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.*;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class FileSource implements SourceFunction<String> {

    /**
     * 文件路径
     */
    private String filePath;

    private InputStream inputStream;

    private BufferedReader reader;

    private Random random = new Random();

    public FileSource(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        inputStream = new FileInputStream(filePath);
        reader = new BufferedReader(new InputStreamReader(inputStream));
        String line = null;
        while ((line = reader.readLine() )!= null){
            // 模拟数据发送
            TimeUnit.MILLISECONDS.sleep(random.nextInt(500));
            ctx.collect(line);
        }
    }

    @Override
    public void cancel() {
        if(reader != null){
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if(inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
