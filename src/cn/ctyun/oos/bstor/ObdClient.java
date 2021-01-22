package cn.ctyun.oos.bstor;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketImpl;

import common.util.GetOpt;
import common.util.JsonUtils;
import common.util.MD5Hash;

/**
 * ObdClient是面向对象的块存储客户端。<p/>
 * 
 * 工作原理: <p/>
 * ObdClient会在Linux内核中注册一个钩子，并指定块设备和套接字，
 * 该钩子会将所有发往该块设备的读写请求全部会通过套接字重定向到服务端，
 * 这样我们就可以向服务端的套接字写入应答来完成块设备的读写请求。<p/>
 * 
 * 使用步骤：<p/>
 * <li>将obd.ko复制到/lib/modules/2.6.32-431.el6.x86_64/build/drivers/block/目录下。这是一个内核模块。 </li>
 * <li>运行"depmod -a"命令，使得系统能感知obd.ko的存在。 </li>
 * <li>将obdhook.so复制到能够被JVM加载的地方。 </li>
 * <li>利用JNI实现ObdClient.hookConnect()这个native方法。
 * 作者本来可以在obdhook.so中实现这一native方法的，但是由于作者不知道ObdClient类未来会在哪个packet下面，
 * 因此，作者在obdhook.so库里面实现了一个名为___hook_connect()的通用函数，
 * 该___hook_connect()函数的参数和返回值跟实现ObdClient.hookConnect()所需要的JNI函数一模一样，
 * 因此当您需要利用JNI实现ObdClient.hookConnect()这个native方法时，只需要调用___hook_connect()函数即可。 </li>
 * <p/>
 * 
 * 注意事项：<p/>
 * 目前只支持对/dev/obd0~15这16个块设备进行处理。如果需要增加，可以联系作者修改一些内核模块参数即可。
 * 
 * 
 * @author Jiang Feng
 *
 */
public class ObdClient {
    
    /**
     * 利用JNI实现ObdClient.startBlkHook这个native方法。作者本来可以在obdhook.so中实现这一native方法的，
     * 但是由于作者不知道ObdClient这个类未来会在哪个packet下面，
     * 因此，作者在obdhook.so库里面实现了一个名为___hook_connect()的通用函数。
     * 该___hook_connect()函数的参数和返回值跟实现startBlkHook()所需要的JNI函数一模一样，
     * 因此当您需要利用JNI实现ObdClient.startBlkHook这个native方法时，只需要调用___hook_connect()函数即可。
     * */
    private static final File NATIVE_LIB = new File("./obdhook.so");

    static {
        System.load(NATIVE_LIB.getAbsolutePath());
    }
    

    /**
     * 在Linux中注册一个钩子，并返回一个套接字描述符(fd)，通过这个fd与设备通信。
     * 该方法适合于将内核中某块设备的请求重定向到本地客户端进程。
     * @param dev 系统设备，目前必须是 /dev/obd0~15. 例如 /dev/obd0, /deb/obd15
     * @param blknum    系统设备的逻辑块个数
     * @param blksize   系统设备的逻辑块大小。大小*个数=该块设备的逻辑大小
     * @return
     */
    static native int hookConnect(String dev, long blknum, long blksize);
    
    /**
     * 将一个已经处于connected状态的套接字sock绑定到设备dev中，并将设备dev设置参数(blknum, blksize)；
     * 该方法适合于讲内核中某块设备的请求重定向于远程服务端进程。
     * @param sock  已经连接到ObdServer的套接字
     * @param dev   系统设备，目前必须是 /dev/obd0~15. 例如 /dev/obd0, /deb/obd15
     * @param blknum    系统设备的逻辑块个数
     * @param blksize   系统设备的逻辑块大小。大小*个数=该块设备的逻辑大小
     */
    static native void hookBind(int sock, String dev, long blknum, long blksize);
    
    /** 
     * 如果设备和ObdClient在协作过程中出现错误，可以重试的次数。
     * 可以改，配置放到配置文件。 
     **/
    private static final int RETRY = 5;
    
    public static class HandshakeReq {
        public String bucket;
        public String devpath;
        public String accessKey;
        public String signature;
        
        public HandshakeReq() {}
        
        public HandshakeReq(String bucket, String devpath, String ak, String sk) {
            this.bucket = bucket;
            this.devpath = devpath;
            this.accessKey = ak;
            this.signature = sign(sk);
        }
        
        public String sign(String sk) {
            return MD5Hash.digest(bucket+":"+devpath+":"+accessKey+":"+sk).toString();
        }
        
    }
    
    public void bind(String host, int port,
            String bucket, String devpath, String ak, String sk,
            String localdev, long blknum, long blksize) throws Exception {
        
        try (Socket sock = new Socket();) {
            sock.connect(new InetSocketAddress(InetAddress.getByName(host), port));
            
            try (DataOutputStream out = new DataOutputStream(sock.getOutputStream());
                 DataInputStream in = new DataInputStream(sock.getInputStream())) {
                
                // 握手
                HandshakeReq req = new HandshakeReq(bucket, devpath, ak,sk);
                out.writeUTF(bucket);
                out.writeUTF(devpath);
                out.writeUTF(ak);
                out.writeUTF(req.signature);
                out.flush();

                int code = in.readInt();
                assert (code == 0); // 表示已经READY

                // 握手完毕
                Field f = Socket.class.getDeclaredField("impl");
                f.setAccessible(true);
                SocketImpl si = (SocketImpl) f.get(sock);

                f = SocketImpl.class.getDeclaredField("fd");
                f.setAccessible(true);
                FileDescriptor desc = (FileDescriptor) f.get(si);

                f = FileDescriptor.class.getDeclaredField("fd");
                f.setAccessible(true);
                int fd = f.getInt(desc);

                hookBind(fd, localdev, blknum, blksize);
            }
        }
    }
    
    /**
     *  ObdClient使用示例
     */
    public static void main(String[] args) throws Exception {
        GetOpt opt = new GetOpt("h:p:[bucket]:[path]:[ak]:[sk]:[dev]:[num]:[size]:", args);
        String host = opt.getOpt("h", "localhost");
        int port = opt.getInt("p", -1);
        String bucket = opt.getOpt("bucket");
        String devpath = opt.getOpt("path");
        String ak = opt.getOpt("ak");
        String sk = opt.getOpt("sk");
        final String dev = opt.getOpt("dev");
        final long blknum = opt.getLong("num", 0);
        final long blksize = opt.getLong("size", 0);
        
        ObdClient client = new ObdClient();
        for (int i = 0; i < RETRY; i++) {
            client.bind(host, port, bucket, devpath, ak, sk, dev, blknum, blksize);
            Thread.sleep(1000); // 让等待内核ready . TODO 可继续研究是否可以去掉等待
        }
        
    }

}
