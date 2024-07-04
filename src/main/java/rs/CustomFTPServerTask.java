package rs;

public class CustomFTPServerTask extends Thread {
    private final CustomFTPServer theCustomFTPServer;

    public CustomFTPServerTask(CustomFTPServer aCustomFTPServer) {
        super();
        theCustomFTPServer = aCustomFTPServer;
    }

    @Override
    public void run() {
        theCustomFTPServer.run();
    }
}
