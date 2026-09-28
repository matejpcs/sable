package dev.ryanhcode.sable.mixin.udp;

import com.llamalad7.mixinextras.sugar.Local;
import dev.ryanhcode.sable.Sable;
import dev.ryanhcode.sable.SableClient;
import dev.ryanhcode.sable.SableConfig;
import dev.ryanhcode.sable.mixinterface.udp.ConnectionExtension;
import dev.ryanhcode.sable.network.udp.SableUDPPacket;
import dev.ryanhcode.sable.network.udp.handler.SableUDPChannelHandlerClient;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import net.minecraft.network.Connection;
import net.minecraft.network.DisconnectionDetails;
import net.minecraft.network.protocol.PacketFlow;
import org.spongepowered.asm.mixin.Mixin;
import org.spongepowered.asm.mixin.Unique;
import org.spongepowered.asm.mixin.injection.At;
import org.spongepowered.asm.mixin.injection.Inject;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfo;
import org.spongepowered.asm.mixin.injection.callback.CallbackInfoReturnable;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Mixin(Connection.class)
public abstract class ConnectionMixin implements ConnectionExtension {

    @Unique
    private Channel sable$udpChannel = null;

    @Unique
    private static final ExecutorService sable$udpBootstrapExecutor = Executors.newCachedThreadPool(runnable -> {
        final Thread thread = new Thread(runnable, "Sable UDP Bootstrap");
        thread.setDaemon(true);
        return thread;
    });

    @Override
    public void sable$setUDPChannel(final Channel channel) {
        this.sable$udpChannel = channel;
    }

    @Inject(method = "disconnect(Lnet/minecraft/network/DisconnectionDetails;)V", at = @At("TAIL"))
    private void sable$onDisconnect(final DisconnectionDetails disconnectionDetails, final CallbackInfo ci) {
        final Channel channel = this.sable$udpChannel;
        if (this.sable$udpChannel != null && this.sable$udpChannel.isOpen()) {
            this.sable$udpChannel = null;

            channel.close().awaitUninterruptibly().addListener((x) -> {
                if (x.isSuccess()) {
                    Sable.LOGGER.debug("[sable-udp] Closed UDP channel");
                } else {
                    Sable.LOGGER.warn("[sable-udp] Failed to close UDP channel", x.cause());
                }
            });
        }
    }

    @Override
    public Channel sable$getUDPChannel() {
        return this.sable$udpChannel;
    }

    @Inject(method = "connect", at = @At("TAIL"))
    private static void sable$connect(final InetSocketAddress inetSocketAddress, final boolean bl, final Connection connection, final CallbackInfoReturnable<ChannelFuture> cir) {
        if (SableConfig.DISABLE_UDP_PIPELINE.get() || !SableConfig.ATTEMPT_UDP_NETWORKING.get()) {
            Sable.LOGGER.debug("[sable-udp] client UDP networking disabled via config; skipping bootstrap for {}", inetSocketAddress);
            return;
        }

        final boolean useNativeTransport = SableClient.useNativeTransport();

        final Class<? extends Channel> channelClass;
        final EventLoopGroup eventLoopGroup;

        if (Epoll.isAvailable() && useNativeTransport) {
            channelClass = EpollDatagramChannel.class;
            eventLoopGroup = Connection.NETWORK_EPOLL_WORKER_GROUP.get();
        } else {
            channelClass = NioDatagramChannel.class;
            eventLoopGroup = Connection.NETWORK_WORKER_GROUP.get();
        }

        /*
         * IMPORTANT: Bootstrap.connect() itself can block inside the OS networking
         * stack on affected Windows machines. Therefore the whole bootstrap call,
         * not merely its completion wait, must stay off the Minecraft login thread.
         */
        sable$udpBootstrapExecutor.execute(() -> {
            final long startNs = System.nanoTime();

            Sable.LOGGER.info("Starting remote client UDP channel future (remote={}, transport={})",
                    inetSocketAddress, channelClass.getSimpleName());

            try {
                final ChannelFuture channelFuture = new Bootstrap()
                        .group(eventLoopGroup)
                        .handler(new ChannelInitializer<>() {
                            @Override
                            protected void initChannel(final Channel channel) {
                                channel.config().setOption(ChannelOption.SO_KEEPALIVE, true);
                                SableUDPPacket.configureSerialization(channel.pipeline(), PacketFlow.CLIENTBOUND, false, null);
                                sable$setupChannel(channel, connection);
                            }
                        })
                        .channel(channelClass)
                        .connect(inetSocketAddress.getAddress(), inetSocketAddress.getPort());

                channelFuture.addListener((ChannelFutureListener) future -> {
                    final long elapsedMs = (System.nanoTime() - startNs) / 1_000_000L;

                    if (future.isSuccess()) {
                        Sable.LOGGER.debug("[sable-udp] UDP connect completed for {} (elapsedMs={})", inetSocketAddress, elapsedMs);
                    } else if (!future.isCancelled()) {
                        Sable.LOGGER.warn("[sable-udp] UDP connect failed for {} (elapsedMs={}): {}",
                                inetSocketAddress,
                                elapsedMs,
                                future.cause() != null ? future.cause().toString() : "<no cause>");
                    }
                });
            } catch (final Throwable t) {
                Sable.LOGGER.error("[sable-udp] Bootstrap.connect threw for {} (elapsedMs={}); continuing without UDP",
                        inetSocketAddress, (System.nanoTime() - startNs) / 1_000_000L, t);
            }
        });
    }

    @Inject(method = "connectToLocalServer", at = @At("TAIL"))
    private static void sable$connectToLocalServer(final SocketAddress socketAddress, final CallbackInfoReturnable<Connection> cir, @Local final Connection connection) {
        if (SableConfig.DISABLE_UDP_PIPELINE.get()) {
            Sable.LOGGER.debug("[sable-udp] local UDP networking disabled via config; skipping bootstrap for {}", socketAddress);
            return;
        }

        sable$udpBootstrapExecutor.execute(() -> {
            final long startNs = System.nanoTime();

            try {
                final ChannelFuture channelFuture = new Bootstrap()
                        .group(Connection.LOCAL_WORKER_GROUP.get())
                        .handler(new ChannelInitializer<>() {
                            @Override
                            protected void initChannel(final Channel channel) {
                                SableUDPPacket.configureInMemoryPipeline(channel.pipeline(), PacketFlow.CLIENTBOUND);
                                sable$setupChannel(channel, connection);
                            }
                        })
                        .channel(LocalChannel.class)
                        .connect(socketAddress);

                channelFuture.addListener((ChannelFutureListener) future -> {
                    final long elapsedMs = (System.nanoTime() - startNs) / 1_000_000L;

                    if (future.isSuccess()) {
                        Sable.LOGGER.debug("[sable-udp] local UDP connect completed for {} (elapsedMs={})",
                                socketAddress, elapsedMs);
                    } else if (!future.isCancelled()) {
                        Sable.LOGGER.warn("[sable-udp] local UDP connect failed for {} (elapsedMs={}): {}",
                                socketAddress,
                                elapsedMs,
                                future.cause() != null ? future.cause().toString() : "<no cause>");
                    }
                });
            } catch (final Throwable t) {
                Sable.LOGGER.error("[sable-udp] local Bootstrap.connect threw for {} (elapsedMs={}); continuing without UDP",
                        socketAddress, (System.nanoTime() - startNs) / 1_000_000L, t);
            }
        });
    }

    @Unique
    private static void sable$setupChannel(final Channel channel, final Connection connection) {
        final ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast(new SableUDPChannelHandlerClient(connection));
    }

}
