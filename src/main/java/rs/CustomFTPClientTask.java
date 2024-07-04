package rs;

public class CustomFTPClientTask extends Thread {
    private final CustomFTPClient theCustomFTPClient;

    public CustomFTPClientTask(CustomFTPClient aCustomFTPClient) {
        super();
        theCustomFTPClient = aCustomFTPClient;
    }

    @Override
    public void run() {
        theCustomFTPClient.run();
    }
}
