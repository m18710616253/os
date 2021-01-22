package cn.ctyun.oos.bssAdapter.server;

//中心节点服务接受的请求来源
public enum SourceTYPE {
    PROXY("PROXY"), MANAGEMENT("MANAGEMENT"), BSS("BSS"), TRUSTRD(
            "TRUSTRD"), ADAPTERSERVER("ADAPTERSERVER");

    // 成员变量
    private String name;

    private SourceTYPE(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
