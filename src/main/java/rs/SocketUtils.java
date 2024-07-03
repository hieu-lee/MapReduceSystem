package rs;

import org.jetbrains.annotations.NotNull;

import java.io.OutputStream;
import java.net.Socket;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.net.Socket;

public class SocketUtils {
    public static final int PORT = 12345;

    public static void write(Socket aSocket, String aMessage) {
        try {
            OutputStream myOutputStream = aSocket.getOutputStream();
            DataOutputStream myDataOutputStream = new DataOutputStream(myOutputStream);
            byte[] myMessageBytes = aMessage.getBytes();
            myDataOutputStream.writeInt(myMessageBytes.length);
            myDataOutputStream.write(myMessageBytes);
            myDataOutputStream.flush();
        } catch (Exception aE) {
            aE.printStackTrace();
        }
    }

    @NotNull
    public static String read(Socket aSocket) {
        try {
            InputStream myInputStream = aSocket.getInputStream();
            DataInputStream myDataInputStream = new DataInputStream(myInputStream);
            int myMessageLength = myDataInputStream.readInt();
            if (myMessageLength > 0) {
                byte[] myBuffer = new byte[myMessageLength];
                myDataInputStream.readFully(myBuffer, 0, myMessageLength);
                return new String(myBuffer, 0, myMessageLength);
            } else {
                return "";
            }
        } catch (Exception aE) {
            aE.printStackTrace();
            return "";
        }
    }
}

