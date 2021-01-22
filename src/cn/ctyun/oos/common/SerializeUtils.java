package cn.ctyun.oos.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cn.ctyun.oos.metadata.UploadMeta;

public class SerializeUtils {
    public static byte[] serializeUploads(List<UploadMeta> uploadList) throws IOException {
        ByteArrayOutputStream byteOS = new ByteArrayOutputStream(uploadList.size() * 12);
        DataOutputStream dos = new DataOutputStream(byteOS);
        for(UploadMeta upload : uploadList) {
            dos.writeInt(upload.partNum);
            dos.writeLong(upload.size);
        }
        return byteOS.toByteArray();
    }
    
    public static List<UploadMeta> deSerializeUploads(byte[] uploadBytes) throws IOException {
        int n = uploadBytes.length / 12;
        List<UploadMeta> list = new ArrayList<>(n);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(uploadBytes));
        for(int i = 0; i < n; i++) {
            UploadMeta um = new UploadMeta();
            um.partNum = dis.readInt();
            um.size = dis.readLong();
            list.add(um);
        }
        return list;
    }
    
    public static void main(String[] args) throws IOException {
        List<UploadMeta> list = new ArrayList<>(15);
        for(int i = 0; i < 15; i++) {
            UploadMeta um = new UploadMeta();
            um.partNum = i;
            um.size = (i + 1) * 1024;
            list.add(um);
            
        }
        byte[] bb = serializeUploads(list);
        System.out.println(bb.length);
        List<UploadMeta> rs = deSerializeUploads(bb);
        for(UploadMeta upload : rs) {
            System.out.println(upload.partNum + ":" + upload.size);
        }
    }
}
