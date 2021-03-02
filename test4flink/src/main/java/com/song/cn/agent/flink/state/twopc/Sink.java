package com.song.cn.agent.flink.state.twopc;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Sink extends TwoPhaseCommitSinkFunction<ObjectNode, Connection, Void> {

//    private Connection connection;

    public Sink() {
        super(new KryoSerializer<>(Connection.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    @Override
    protected void invoke(Connection connection, ObjectNode objectNode, Context context) throws Exception {
        String stu = objectNode.get("value").toString();
        Student student = JSON.parseObject(stu, Student.class);

        System.err.println("start invoke......." + "id = " + student.getId() + "  name = " + student.getName() + "   password"
                + " = " + student.getPassword() + "  age = " + student.getAge());

        String sql = "insert into Student(id,name,password,age) values (?,?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setInt(1, student.getId());
        ps.setString(2, student.getName());
        ps.setString(3, student.getPassword());
        ps.setInt(4, student.getAge());
        ps.executeUpdate();
        //手动制造异常
        if (student.getId() == 33) {
            System.out.println(1 / 0);
        }
    }

    @Override
    protected Connection beginTransaction() throws Exception {
        String url = "jdbc:mysql://192.168.80.235:3306/test?useUnicode=true&characterEncoding=UTF-8"
                + "&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true";
        return DBConnectUtil.getConnection(url, "root", "123456");
    }

    @Override
    protected void preCommit(Connection connection) throws Exception {
    }

    @Override
    protected void commit(Connection connection) {
        if (connection != null) {
            try {
                connection.commit();
            } catch (SQLException e) {
                System.err.println("commit  error ............" + e.getMessage());
            } finally {
                try {
                    connection.close();
                } catch (SQLException e) {
                    System.err.println(" finally  commit error ............" + e.getMessage());
                }
            }
        }
    }

    @Override
    protected void abort(Connection connection) {
        if (connection != null) {
            try {
                connection.rollback();
            } catch (SQLException e) {
                System.err.println("abort error ............" + e.getMessage());
            } finally {
                try {
                    connection.close();
                } catch (SQLException e) {
                    System.err.println(" finally  abort error ............" + e.getMessage());
                }
            }
        }
    }
}

