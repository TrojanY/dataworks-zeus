package com.taobao.zeus.socket.master;

import java.net.InetSocketAddress;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

import com.taobao.zeus.schedule.mvc.ScheduleInfoLog;
import com.taobao.zeus.socket.protocol.Protocol;


public class MasterServer{
	// Server服务启动器
	private ServerBootstrap bootstrap;
	private ChannelFuture channelFuture;
	private EventLoopGroup bossGroup = new NioEventLoopGroup();
	private EventLoopGroup workerGroup = new NioEventLoopGroup();
	private final ProtobufVarint32LengthFieldPrepender frameEncoder = new ProtobufVarint32LengthFieldPrepender();
	private final ProtobufEncoder protobufEncoder = new ProtobufEncoder();
	
	MasterServer(final ChannelHandler handler){

		bootstrap=new ServerBootstrap();

		bootstrap.group(bossGroup, workerGroup)
				.channel(NioServerSocketChannel.class)
				.childHandler(new ChannelInitializer<SocketChannel>() {
					@Override
					public void initChannel(SocketChannel ch) throws Exception {
						ChannelPipeline pipeline = ch.pipeline();
						pipeline.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
						pipeline.addLast("protobufDecoder",new ProtobufDecoder(Protocol.SocketMessage.getDefaultInstance()));
						pipeline.addLast("frameEncoder", frameEncoder);
						pipeline.addLast("protobufEncoder", protobufEncoder);
						pipeline.addLast("handler", handler);
					}
				})
				.option(ChannelOption.SO_BACKLOG, 128)
				.childOption(ChannelOption.SO_KEEPALIVE, true);
	}
	
	public synchronized void start(int port) throws InterruptedException {
		channelFuture = bootstrap.bind(new InetSocketAddress(port)).sync();
		ScheduleInfoLog.info("zeus schedule server start");
	}
	
	synchronized void shutdown(){
		try {
			channelFuture.channel().closeFuture().sync();
		} catch (InterruptedException e) {
			workerGroup.shutdownGracefully();
			bossGroup.shutdownGracefully();
		}
		ScheduleInfoLog.info("zeus schedule server shutdown");
	}
}
