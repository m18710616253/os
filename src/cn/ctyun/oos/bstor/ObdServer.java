package cn.ctyun.oos.bstor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Set;

import cn.ctyun.oos.bstor.ObdClient.HandshakeReq;
import common.time.TimeStat;
import common.tuple.Triple;
import common.util.BlockingExecutor;


/**
 * BUG 客户端会出现bad descriptor这样的错误
 * @author Jiang
 *
 */
public class ObdServer {
    
    public static final int DEFAULT_PORT = 20000;
    
    public static final int ERR_AUTH_FAIL = 1;

    public static class BlkReq {
        public static final int MAGIC = 0x25609513;
        public static final int TYPE_CMD_READ = 0;
        public static final int TYPE_CMD_WRITE = 1;
        public static final int TYPE_CMD_DISC = 2;
        public static final int TYPE_CMD_SEND_FLUSH = 3;
        public static final int TYPE_CMD_SEND_TRIM = 4;
        
        private int magic;
        private int type;
        private long handle; //char[8]
        public long from;
        public int len;
        
        void read(DataInputStream in) throws IOException {
            magic = in.readInt();
            assert(magic == MAGIC);
            type = in.readInt();
            handle = in.readLong();
            from = in.readLong();
            len = in.readInt();
        }
    }

    public static class BlkReply {
        public static final int MAGIC = 0x67446698;
        
        private int magic = MAGIC;
        public int error;
        private long handle; //char[8]
        
        void write(DataOutputStream out) throws IOException {
            out.writeInt(magic);
            out.writeInt(error);
            out.writeLong(handle);
        }
    }
    
    /**
     * IOHandler接口。
     * 为了不阻塞内部主循环，应该用异步线程去执行该任务，然后向out中写入reply头和数据体。
     * 需要注意的是，对out写入数据必须是原子的，reply头和数据体中间不能有其他内容。 
     *
     */
    public static interface IOHandler {

        boolean validata(HandshakeReq handshake);

        void setIO(Socket sock, DataInputStream in, DataOutputStream out);

        /** 
         * 客户端需要从偏移量from字节开始读len字节的数据。<p/>
         */
        void read(BlkReply reply, long from, int len);
        
        /**
         * 客户端需要从偏移量from字节开始写入len字节的数据。<p/>
         * @throws IOException 
         */
        void write(BlkReply reply, long from, int len) throws IOException;
        void flush(BlkReply reply) throws Exception;
        void trim(BlkReply reply) throws Exception;
        void disc();
    }

    private void handleReq(Socket sock, final IOHandler h) throws Exception {
        // 需要用BufferedOutputStream，这样会大大减小底层的write(fd)次数，从而减小cpu sys态开销
        // 但是不要buffer越大越好，java默认的buffer size即可。
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(sock.getInputStream()));
             DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(sock.getOutputStream()))) {
            
            // 握手
            HandshakeReq hs = new HandshakeReq();
            hs.bucket = in.readUTF();
            hs.devpath = in.readUTF();
            hs.accessKey = in.readUTF();
            hs.signature = in.readUTF();
            if(h.validata(hs)) {
                out.writeInt(0); // 告诉客户端READY
                out.flush();
            }else {
                out.writeInt(ERR_AUTH_FAIL);
                out.flush();
                return;
            }
            // 握手完毕
            h.setIO(sock, in, out);
            
            final BlkReq req = new BlkReq();
            while (true) {
                req.read(in);
                switch (req.type) {
                case BlkReq.TYPE_CMD_READ: {
                    final BlkReply reply = new BlkReply();
                    reply.handle = req.handle;
                    h.read(reply, req.from, req.len);
                    break;
                }
                    
                case BlkReq.TYPE_CMD_WRITE: {
                    final BlkReply reply = new BlkReply();
                    reply.handle = req.handle;
                    h.write(reply, req.from, req.len);
                    break;
                }
                    
                case BlkReq.TYPE_CMD_DISC:
                    h.disc();
                    break;
                    
                case BlkReq.TYPE_CMD_SEND_FLUSH: {
                    BlkReply reply = new BlkReply();
                    reply.handle = req.handle;
                    h.flush(reply);
                    break;
                }
                    
                case BlkReq.TYPE_CMD_SEND_TRIM: {
                    BlkReply reply = new BlkReply();
                    reply.handle = req.handle;
                    h.trim(reply);
                    break;
                }
                    
                default:
                    assert(false);
                }
            }
        }
    }
    
    public ObdServer() {
    }
    
    public void start(int port, final IOHandler h) throws Exception {
        try(ServerSocket ss = new ServerSocket(port);) {
            for(;;) {
                final Socket sock = ss.accept();
                System.out.println("received a tcp connection. socket = " + sock);
                
                //Socket层不缓存数据，提高服务端响应速度；
                //但是应用层用BufferedInputStream来手工flush，降低网络中的碎片
                sock.setTcpNoDelay(true); 
                new Thread() {
                    {
                        this.setName("Acceptor");
                    }
                    public void run() {
                        try {
                            handleReq(sock, h);
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            try { sock.close(); } catch (IOException e) { }
                        }
                    };
                }.start();
            }
        }
    }

    public static class MyIOHandler implements IOHandler {
        
        final int SYS_SECT_SIZE = 512; //Linux操作系统的最小sector大小
        
        final int SECT_SIZE = 1024*1024;  // OBS中sector的单位
        
        final byte[] UNINITIALIZED = new byte[SECT_SIZE];
        
        File devdir;
        
        Socket sock;
        
        DataInputStream in;
        
        DataOutputStream out;
        
        // 线程池的大小可以随意调节。也可以放到配置文件中
        private BlockingExecutor thrpool = new BlockingExecutor(4, 16, 16, 4000, "ioexecutor");
        
        @Override
        public boolean validata(HandshakeReq handshake) {
            // TODO 这里应该查询meta库获得secret
            String secret = "secretKey";
            boolean success = handshake.signature.equals(handshake.sign(secret));
            if(!success)
                return false;
            
            File root = new File("./obd-dir/");
            devdir = new File(new File(root, handshake.bucket), handshake.devpath);
            devdir.mkdirs();
            
            success = makeRoot(handshake.bucket, handshake.devpath);
            if(!success)
                return false;
            
            new WriteThread().start();
            return true;
        }
        
        protected boolean makeRoot(String bucket, String devpath) {
            File root = new File("./obd-dir/");
            devdir = new File(new File(root, bucket), devpath);
            devdir.mkdirs();
            
            return true;
        }
        
        @Override
        public void setIO(Socket sock, DataInputStream in, DataOutputStream out) {
            this.sock = sock;
            this.in = in;
            this.out = out;
        }
        
        private void closeIO() {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                sock.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        static class ReadCache extends LinkedHashMap<Long, byte[]> {

            private static final long serialVersionUID = 3743374992278058790L;
            
            static final int CACHE_SIZE = 10;
            
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, byte[]> eldest) {
                return (super.size() > CACHE_SIZE);
            }
        }
        
        ReadCache readcache = new ReadCache();
        
        protected byte[] readSector(long i) throws IOException {
            File f = new File(devdir, Long.toHexString(i));
            try (FileInputStream fis = new FileInputStream(f) ){
                byte[] data = new byte[SECT_SIZE];
                fis.read(data, 0, data.length);
                return data;
            } catch (FileNotFoundException e) {
                return UNINITIALIZED; //表示这个sector原本就没有数据
            }
        }
        
        @Override
        public void read(final BlkReply reply, final long from, final int len) {
            //from和len必须是512字节的整数倍
            assert( (from & (SYS_SECT_SIZE-1)) == 0);
            assert( (len & (SYS_SECT_SIZE-1)) == 0);
            
            // 异步的读出数据，然后返回给客户端
            thrpool.execute(new Runnable() {

                @Override
                public void run() {
                    Map<Long, byte[]> map = new HashMap<>();
                    final long sect_from = from / SECT_SIZE;
                    final long sect_end = (from+len-1)/SECT_SIZE;
                    boolean error = false;
                    for(long i=sect_from; i<=sect_end; i++) {
                        byte[] content = null;
                        boolean hit;
                        synchronized (readcache) {
                            hit = readcache.containsKey(i);
                            content = readcache.remove(i); //拿出去再放进来
                            if(hit)
                                readcache.put(i, content);
                        }
                        if(hit) {
                            assert(content != null);
                            map.put(i, content);
                        } else {
                            try {
                                byte[] data = readSector(i);
                                synchronized (readcache) { //尽快放入cache以便提高其他线程的命中率
                                    readcache.put(i, data);
                                }
                                map.put(i, data);
                            } catch (IOException e) {
                                e.printStackTrace();
                                error = true;
                                break;
                            } 
                        }
                    }
                    
                    if (error) { //如果有错误
                        try {
                            reply.error = 1;
                            synchronized (out) {
                                reply.write(out);
                            }
                            out.flush();
                        } catch (Exception e) {
                            e.printStackTrace();
                            closeIO();
                        }
                        return;
                    } 

                    assert(map.size() == (sect_end - sect_from + 1)); //全部读满了
                    
                    // 写出数据
                    reply.error = 0;
                    try {
                        synchronized (out) {
                            reply.write(out);
                            for (long i = sect_from; i <= sect_end; i++) {
                                // 找到在sec_i中的左右边界
                                long sect_left = i * SECT_SIZE;
                                long left = Math.max(sect_left, from);
                                long right = Math.min(sect_left + SECT_SIZE, from + len);

                                byte[] data = map.get(i);
                                assert(data != null);
                                out.write(data, (int)(left-sect_left), (int)(right-left));
                            }
                        }
                        out.flush();
                        System.out.println("Read: from=" + from + " len=" + len);
                    } catch (Exception e) {
                        e.printStackTrace();
                        closeIO();
                    }
                    
                }
            });
        }
        
        
        static class WriteBuffer extends LinkedHashMap<Long, Triple<BitSet, byte[], Set<BlkReply>>> {

            private static final long serialVersionUID = -1290380058080105770L;
            
            static final int WRITE_BUFF_SIZE = 10;
            
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, Triple<BitSet, byte[], Set<BlkReply>>> eldest) {
                // 如果已经超过了上限，则一直等
                while(super.size() > WRITE_BUFF_SIZE) {
                    try {
                        synchronized (WriteBuffer.this) {
                            this.wait(1);
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                return false;
            }
            
        }
        
        // 只有一个线程写，只有一个线程读
        WriteBuffer writeBuffer = new WriteBuffer();
        volatile int writeReq = 0;
        volatile int writeResp = 0;
        
        @Override
        public void write(final BlkReply reply, final long from, final int len) throws IOException {
            // from和len必须是512字节的整数倍
            assert( (from & (SYS_SECT_SIZE-1)) == 0);
            assert( (len & (SYS_SECT_SIZE-1)) == 0);
            
            System.out.println("GET write request: req=" + ++writeReq + " , resp=" + writeResp);
            
            //从in里面读出数据。这个地方没有必要异步读，因为都是内存操作。
            
            final long sect_from = from / SECT_SIZE;
            final long sect_end = (from+len-1)/SECT_SIZE;
            
            for (long i = sect_from; i <= sect_end; i++) {
                // 找到在sec_i中的左右边界
                long sect_left = i * SECT_SIZE;
                long left = Math.max(sect_left, from);
                long right = Math.min(sect_left + SECT_SIZE, from + len);
                
                Triple<BitSet, byte[], Set<BlkReply>> triple;
                synchronized (writeBuffer) {
                    triple = writeBuffer.get(i);
                }
                
                if(triple == null) {
                    byte[] data = new byte[SECT_SIZE];
                    in.readFully(data, (int)(left-sect_left), (int)(right-left));
                    
                    BitSet bs = new BitSet(SECT_SIZE/SYS_SECT_SIZE);
                    bs.set((int)(left-sect_left)/SYS_SECT_SIZE, (int)(right-sect_left)/SYS_SECT_SIZE);
                    
                    Set<BlkReply> replySet = new HashSet<BlkReply>();
                    replySet.add(reply);
                    
                    triple = new Triple<>(bs, data, replySet);
                    synchronized (writeBuffer) {
                        writeBuffer.put(i, triple);
                    }
                } else {
                    synchronized (triple) {
                        byte[] data = triple.second();
                        in.readFully(data, (int)(left-sect_left), (int)(right-left));
                        
                        BitSet bs = triple.first();
                        bs.set((int)(left-sect_left)/SYS_SECT_SIZE, (int)(right-sect_left)/SYS_SECT_SIZE);
                        
                        Set<BlkReply> replySet = triple.third();
                        synchronized (replySet) {
                            replySet.add(reply);
                        }
                    }
                    
                    synchronized (writeBuffer) {
                        if(!writeBuffer.containsKey(i))
                            writeBuffer.put(i, triple);
                    }
                }
            }
        }
        
        protected void writeSector(long i, byte[] data) throws IOException {
            File f = new File(devdir, Long.toHexString(i));
            try (FileOutputStream fis = new FileOutputStream(f) ){
                fis.write(data, 0, data.length);
            }
        }
        
        class WriteThread extends Thread {
            public WriteThread() {
                this.setName("WriteThread");
            }
            @Override
            public void run() {
                for(;;) {
                    
                    Entry<Long, Triple<BitSet, byte[], Set<BlkReply>>> head = null;
                    synchronized (writeBuffer) {
                        Iterator<Entry<Long, Triple<BitSet, byte[], Set<BlkReply>>>> itor
                            = writeBuffer.entrySet().iterator();
                        if(itor.hasNext()) {
                            head = itor.next();
                            assert(head != null);
                            itor.remove();
                        }
                    }
                    if(head == null) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        continue;
                    }
                    
                    System.out.println("Pooling a writing request.");
                    
                    synchronized (head) {
                        boolean directWrite = false;
                        BitSet bs = head.getValue().first();
                        if(bs.cardinality() == SECT_SIZE/SYS_SECT_SIZE) {
                            // 说明这个section全部被写满，可以直接写存储
                            directWrite = true;
                        }
                        if(directWrite)
                            directWrite(head);
                        
                        else {
                            // head还在writeBuffer里面，此时执行Read-Mod-Write
                            // 先看看cache有没有
                            byte[] data_old = null;
                            synchronized (readcache) {
                                data_old = readcache.get(head.getKey());
                            }
                            if(data_old == null) {
                                try {
                                    data_old = readSector(head.getKey());
                                } catch (IOException e) {
                                    writeResponse(head, 1);
                                    continue;
                                }
                            }
                            
                            // 将两路cache进行合并
                            byte[] data_new = head.getValue().second();
                            for(int i=0; i<SECT_SIZE/SYS_SECT_SIZE; i++) {
                                if(bs.get(i) ==false) 
                                    System.arraycopy(data_old, i*SYS_SECT_SIZE, data_new, i*SYS_SECT_SIZE, SYS_SECT_SIZE);
                            }
                            directWrite(head);
                        }
                    }
                    
                }
            }
            
            private void writeResponse(Entry<Long, Triple<BitSet, byte[], Set<BlkReply>>> head, int error) {
                synchronized (out) {
                    Set<BlkReply> set = head.getValue().third();
                    synchronized (set) {
                        Iterator<BlkReply> itor = set.iterator();
                        while(itor.hasNext()) {
                            BlkReply reply = itor.next();
                            itor.remove();
                            reply.error = error;
                            try {
                                reply.write(out);
                                System.out.println("FINISH write request: req=" + writeReq + " , resp=" + ++writeResp);
                            } catch (IOException e1) {
                                e1.printStackTrace();
                                closeIO();
                            }
                        }
                    }
                }
                try {
                    out.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                    closeIO();
                }
            }
            
            private void directWrite(Entry<Long, Triple<BitSet, byte[], Set<BlkReply>>> head) {
                try {
                    writeSector(head.getKey(), head.getValue().second());
                    
                    // 更新读缓存，如果缓存里面有的话
                    synchronized (readcache) {
                        if(readcache.containsKey(head.getKey()))
                            readcache.put(head.getKey(), head.getValue().second());
                    }
                    writeResponse(head, 0);
                } catch (IOException e) {
                    writeResponse(head, 1);
                }
            }
            
        }
        
        @Override
        public void flush(BlkReply reply) throws Exception {
            reply.error = 0;
            // 确保out操作原子性即可
            synchronized(out) {
                reply.write(out);
            }
            out.flush();
            System.err.println("*** flush ***");
        }

        @Override
        public void trim(BlkReply reply) throws Exception {
            reply.error = 0;
            // 确保out操作原子性即可
            synchronized(out) {
                reply.write(out);
            }     
            out.flush();
            System.err.println("*** trim ***");
        }
        
        @Override
        public void disc() {
            System.err.println("disc");
        }
    }


    public static void main(String[] args) throws Exception {
//        GetOpt opt = new GetOpt("d:p:n:s:", args);
//        final String dev = opt.getOpt("d");
//        final long blknum = opt.getLong("n", 0);
//        final long blksize = opt.getLong("s", 0);
        final long blknum = 40000;
        final long blksize = 4096;
        
        new ObdServer().start(DEFAULT_PORT, new MyIOHandler());
    }

}
