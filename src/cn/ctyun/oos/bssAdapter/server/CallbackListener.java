package cn.ctyun.oos.bssAdapter.server;

import java.io.IOException;

import cn.ctyun.common.BaseException;

public interface CallbackListener {
    void onComplete(int code) throws BaseException, IOException;
}
