package cn.ctyun.oos.server.backoff;

public class BandwidthBackoff {
    public boolean readPublicBackoff = false;
    public boolean writePublicBackoff = false;
    public boolean readPrivateBackoff = false;
    public boolean writePrivateBackoff = false;
}
