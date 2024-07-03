package rs;

public class CustomFTPCredential {
    private final String theUsername;
    private final String thePassword;
    private final int thePort;
    private static CustomFTPCredential theFtpCredential;

    private CustomFTPCredential(String aUsername, String aPassword, int aPort) {
        theUsername = aUsername;
        thePassword = aPassword;
        thePort = aPort;
    }

    public static CustomFTPCredential getInstance() {
        if (theFtpCredential == null) {
            theFtpCredential = new CustomFTPCredential("hieuleeeeeee", "linhvuuuuuuu", 2254);
        }
        return theFtpCredential;
    }

    public String getUsername() {
        return theUsername;
    }

    public String getPassword() {
        return thePassword;
    }

    public int getPort() {
        return thePort;
    }
}
