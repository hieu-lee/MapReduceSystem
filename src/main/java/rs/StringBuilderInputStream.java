package rs;

import java.io.IOException;
import java.io.InputStream;

public class StringBuilderInputStream extends InputStream {
    private final StringBuilder theStringBuilder;
    private int thePosition;

    public StringBuilderInputStream(StringBuilder aStringBuilder) {
        theStringBuilder = aStringBuilder;
        thePosition = 0;
    }

    @Override
    public int read() throws IOException {
        int myLength = theStringBuilder.length();
        if (thePosition < myLength) {
            return theStringBuilder.charAt(thePosition++);
        } else {
            return -1;
        }
    }

    @Override
    public int read(byte[] aBytes, int aOff, int aLength) throws IOException {
        int myLength = theStringBuilder.length();
        if (thePosition >= myLength) {
            return -1; // End of stream
        }
        int myBytesRead = 0;
        for (int i = 0; i < aLength && thePosition < myLength; i++) {
            aBytes[aOff + i] = (byte) theStringBuilder.charAt(thePosition++);
            myBytesRead++;
        }
        return myBytesRead;
    }
}
