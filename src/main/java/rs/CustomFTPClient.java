package rs;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.stream.Collectors;

public class CustomFTPClient {
    private final String theFilename;
    private final String theFileContent;
    private final String theServer;
    private final int thePort;
    private final String theUsername;
    private final String thePassword;
    private final FTPClient theFtpClient;
    private final CustomFTPClientType theCustomFTPClientType;
    private final Path thePath;

    public CustomFTPClient(String aFilename, String aFileContent, String aServer, CustomFTPCredential aCredential, CustomFTPClientType aCustomFTPClientType) {
        theCustomFTPClientType = aCustomFTPClientType;
        theFilename = aFilename;
        theFileContent = aFileContent;
        theServer = aServer;
        thePort = aCredential.getPort();
        theUsername = aCredential.getUsername();
        thePassword = aCredential.getPassword();
        theFtpClient = new FTPClient();
        thePath = null;
    }

    public CustomFTPClient(String aFilename, Path aPath, String aServer, CustomFTPCredential aCredential, CustomFTPClientType aCustomFTPClientType) {
        theCustomFTPClientType = aCustomFTPClientType;
        theFilename = aFilename;
        theFileContent = null;
        theServer = aServer;
        thePort = aCredential.getPort();
        theUsername = aCredential.getUsername();
        thePassword = aCredential.getPassword();
        theFtpClient = new FTPClient();
        thePath = aPath;
    }

    public CustomFTPClient(String aFilename, String aFileContent, String aServer, CustomFTPCredential aCredential) {
        this(aFilename, aFileContent, aServer, aCredential, CustomFTPClientType.STORE);
    }

    public CustomFTPClient(String aFilename, String aServer, CustomFTPCredential aCredential) {
        theFilename = aFilename;
        theFileContent = null;
        theServer = aServer;
        thePort = aCredential.getPort();
        theUsername = aCredential.getUsername();
        thePassword = aCredential.getPassword();
        theFtpClient = new FTPClient();
        theCustomFTPClientType = CustomFTPClientType.MASTER;
        thePath = null;
    }

    private boolean filenameExisted() {
        try {
            FTPFile[] myFiles = theFtpClient.listFiles();
            boolean myFileExisted = false;
            for (FTPFile aFile : myFiles) {
                if (aFile.getName().equals(theFilename)) {
                    myFileExisted = true;
                    break;
                }
            }
            return myFileExisted;
        }
        catch (IOException aE) {
            aE.printStackTrace();
            return false;
        }
    }

    public void storeFile(String aFileContent) {
        try (InputStream myInputStream = new ByteArrayInputStream(aFileContent.getBytes())) {
            theFtpClient.storeFile(theFilename, myInputStream);
        }
        catch (IOException aE) {
            aE.printStackTrace();
        }
        int myErrorCode = theFtpClient.getReplyCode();
        if (myErrorCode != 226) {
            System.out.println("File upload failed. FTP Error code: " + myErrorCode);
        }
    }

    public void storeFile(StringBuilder aFileContent) {
        try (InputStream myInputStream = new StringBuilderInputStream(aFileContent)) {
            theFtpClient.storeFile(theFilename, myInputStream);
        }
        catch (IOException aE) {
            aE.printStackTrace();
        }
        int myErrorCode = theFtpClient.getReplyCode();
        if (myErrorCode != 226) {
            System.out.println("File upload failed. FTP Error code: " + myErrorCode);
        }
    }

    public void appendFile(String aFileContent) {
        try (InputStream myInputStream = new ByteArrayInputStream(aFileContent.getBytes())) {
            if (!theFtpClient.appendFile(theFilename, myInputStream)) {
                System.err.println("Failed to append file.");
            }
        } catch (IOException aE) {
            aE.printStackTrace();
        }
    }

    public void appendFile(StringBuilder aFileContent) {
        try (InputStream myInputStream = new StringBuilderInputStream(aFileContent)) {
            if (!theFtpClient.appendFile(theFilename, myInputStream)) {
                System.err.println("Failed to append file.");
            }
        } catch (IOException aE) {
            aE.printStackTrace();
        }
    }

    private void storeFile() {
        if (thePath == null) {
            try (InputStream myInputStream = new ByteArrayInputStream(theFileContent.getBytes())) {
                theFtpClient.storeFile(theFilename, myInputStream);
            }
            catch (IOException aE) {
                aE.printStackTrace();
            }
        } else {
            try (InputStream myInputStream = Files.newInputStream(thePath)) {
                theFtpClient.storeFile(theFilename, myInputStream);
            }
            catch (IOException aE) {
                aE.printStackTrace();
            }
        }
        int myErrorCode = theFtpClient.getReplyCode();
        if (myErrorCode != 226) {
            System.out.println("File upload failed. FTP Error code: " + myErrorCode);
        } else {
            System.out.println("File uploaded successfully.");
        }
    }

    public void setUpClient() {
        try {
            theFtpClient.connect(theServer, thePort);
            theFtpClient.login(theUsername, thePassword);
            theFtpClient.enterLocalPassiveMode();
            theFtpClient.setFileStructure(FTP.BINARY_FILE_TYPE);
        } catch (IOException aE) {
            aE.printStackTrace();
        }
    }

    private void displayFileContent() {
        try {
            InputStream myInputStream = theFtpClient.retrieveFileStream(theFilename);
            BufferedReader myReader = new BufferedReader(new InputStreamReader(myInputStream));
            Path myOutputPath = Paths.get("output.txt");
            Files.write(myOutputPath, myReader.lines().collect(Collectors.toList()), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            myReader.close();
            theFtpClient.completePendingCommand();
        } catch (IOException aE) {
            aE.printStackTrace();
        }
    }

    private void appendFileContent() {
        if (thePath != null) {
            try (InputStream myInputStream = Files.newInputStream(thePath)) {
                if (theFtpClient.appendFile(theFilename, myInputStream)) {
                    System.out.println("File appended successfully!");
                } else {
                    System.err.println("Failed to append file.");
                }
            } catch (IOException aE) {
                aE.printStackTrace();
            }
        }
        else {
            try (InputStream myInputStream = new ByteArrayInputStream(theFileContent.getBytes())) {
                if (theFtpClient.appendFile(theFilename, myInputStream)) {
                    System.out.println("File appended successfully!");
                } else {
                    System.err.println("Failed to append file.");
                }
            } catch (IOException aE) {
                aE.printStackTrace();
            }
        }
    }

    public void logoutAndDisconnect() {
        try {
            theFtpClient.logout();
            theFtpClient.disconnect();
        } catch (IOException aE) {
            aE.printStackTrace();
        }
    }

    private void deleteFile() {
        try {
            if (theFtpClient.deleteFile(theFilename)) {
                System.out.println("File deleted successfully.");
            } else {
                System.err.println("Failed to delete file.");
            }
        } catch (IOException aE) {
            aE.printStackTrace();
        }
    }

    private void deleteFileIfExist() {
        if (filenameExisted()) {
            deleteFile();
        }
    }

    public void clientDisplayFile() {
        setUpClient();
        if (filenameExisted()) {
            displayFileContent();
        } else {
            System.err.println("File does not exist.");
        }
        logoutAndDisconnect();
    }

    private void clientStoreFile() {
        setUpClient();
        storeFile();
        logoutAndDisconnect();
    }

    private void clientAppendFile() {
        setUpClient();
        appendFileContent();
        logoutAndDisconnect();
    }

    private void clientDeleteFile() {
        setUpClient();
        deleteFileIfExist();
        logoutAndDisconnect();
    }

    public void run() {
        switch (theCustomFTPClientType) {
            case STORE:
                clientStoreFile();
                break;
            case APPEND:
                clientAppendFile();
                break;
            case DISPLAY:
                clientDisplayFile();
                break;
            case DELETE:
                clientDeleteFile();
                break;
            default:
                System.err.println("Invalid FTP client type.");
        }
    }
}
