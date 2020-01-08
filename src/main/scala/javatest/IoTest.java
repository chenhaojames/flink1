package javatest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

/**
 * @author chenhao
 * @description <p>
 * created by chenhao 2020/1/4 16:04
 */
public class IoTest {
    public static void main(String[] args) throws IOException {
        URL url = new URL("http://192.168.120.17:9003/?/scenevehicles/dn@node17/10/96-23c35a64-19c31");
        URLConnection urlConnection = url.openConnection();
        urlConnection.connect();
        InputStream inputStream = urlConnection.getInputStream();
        byte[] bytes = new byte[urlConnection.getContentLength()];
        inputStream.read(bytes,0,urlConnection.getContentLength());
        System.out.println(bytes.length);
    }
}
