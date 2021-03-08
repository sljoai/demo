package com.song.cn.hbase.mr.reducer;

import com.song.cn.hbase.mr.bean.CacheData;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLOutputFormat extends OutputFormat<Text,CacheData> {

    private FileOutputCommitter committer = null;

    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new MySQLRecordWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if(committer == null){
            Path name = getOutputPath(context);
            committer = new FileOutputCommitter(name,context);
        }
        return committer;
    }

    private static Path getOutputPath(JobContext job){
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null:new Path(name);
    }


    class MySQLRecordWriter extends RecordWriter<Text, CacheData> {

        private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";

        private static final String MYSQL_URL = "jdbc:mysql://hadoop01/student?useUnicode=true&characterEncoding=UTF-8&useSSL=false";

        private static final String MYSQL_USERNAME = "root";

        private static final String MYSQL_PASSWORD = "123456";

        private Connection connection;

        public MySQLRecordWriter() {
            try {
                Class.forName(MYSQL_DRIVER_CLASS);
                connection = DriverManager.getConnection(MYSQL_URL,MYSQL_USERNAME,MYSQL_PASSWORD);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }

        }

        @Override
        public void write(Text key, CacheData value) throws IOException, InterruptedException {
            String sql = "insert into statistic_result(name,sum_cnt) values(?,?)";

            PreparedStatement preparedStatement = null;

            try {
                preparedStatement = connection.prepareStatement(sql);
                preparedStatement.setObject(1,key.toString());
                preparedStatement.setObject(2,value.getCount());
                preparedStatement.execute();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }finally {
                if(preparedStatement != null){
                    try {
                        preparedStatement.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                if(connection != null){
                    try {
                        connection.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }
        }
    }
}
