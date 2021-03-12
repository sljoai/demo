package com.song.cn.agent.grpc;

import io.grpc.stub.StreamObserver;

public class StudentServiceImpl extends StudentServiceGrpc.StudentServiceImplBase {

    @Override
    public void getRealNameByUsername(MyRequest request, StreamObserver<MyResponse> responseObserver) {
        System.out.println("接受到客户端信息：" + request.getUsername());

        responseObserver.onNext(
                MyResponse.newBuilder()
                        .setRealName("张三")
                        .build());
        responseObserver.onCompleted();
    }
}
