package com.zwq.infinity.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.regex.Pattern;

@Slf4j
public class NetworkUtil {
    private static final Pattern IP_PATTERN = Pattern.compile("\\d{1,3}(\\.\\d{1,3}){3,5}$");
    public static final String LOCALHOST = "127.0.0.1";
    public static final String ANYHOST = "0.0.0.0";

    private static volatile InetAddress LOCAL_ADDRESS = null;

    public static String getHostIp() {
        if (LOCAL_ADDRESS == null) {
            LOCAL_ADDRESS = getLocalAddress0();
        }
        return LOCAL_ADDRESS == null ? LOCALHOST : LOCAL_ADDRESS.getHostAddress();
    }
    public static String getHostName() {
        if (LOCAL_ADDRESS == null) {
            LOCAL_ADDRESS = getLocalAddress0();
        }
        return LOCAL_ADDRESS == null ? LOCALHOST : LOCAL_ADDRESS.getHostName();
    }

    /**
     * 遍历本地网卡,返回第一个合理的IP
     *
     * @return 本地网卡IP
     */
    private static InetAddress getLocalAddress0() {
        //先尝试直接拿
        try {
            InetAddress localAddress = InetAddress.getLocalHost();
            if (isValidAddress(localAddress)) {
                return localAddress;
            }
        } catch (UnknownHostException e) {
            log.info("get local ip address error:{}", ExceptionUtils.getStackTrace(e));
        }
        //拿不到取第一个有效ip
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                Enumeration<InetAddress> addresses = interfaces.nextElement().getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (isValidAddress(address)) {
                        return address;
                    }
                }
            }
        } catch (Exception e) {
            log.info("get local ip address error:{}", ExceptionUtils.getStackTrace(e));
        }

        log.error("Could not get local host ip address, will use " + LOCALHOST + " instead.");
        return null;
    }


    private static boolean isValidAddress(InetAddress address) {
        if (address == null || address.isLoopbackAddress()) {
            return false;
        }
        String name = address.getHostAddress();
        return name != null && !ANYHOST.equals(name) && !LOCALHOST.equals(name) && IP_PATTERN.matcher(name).matches();
    }


}
