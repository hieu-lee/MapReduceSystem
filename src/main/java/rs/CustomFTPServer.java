package rs;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.ClearTextPasswordEncryptor;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.apache.log4j.PropertyConfigurator;

public class CustomFTPServer extends Thread {
    private final FtpServer theFtpServer;
    private final FtpServerFactory theFtpServerFactory;
    private final ListenerFactory theListenerFactory;
    private final String theHomeDirectory;
    private final int thePort;

    public CustomFTPServer(CustomFTPCredential aFtpCredential) {
        super();
        PropertyConfigurator.configure(CustomFTPServer.class.getResource("/log4J.properties"));
        theFtpServerFactory = new FtpServerFactory();
        thePort = aFtpCredential.getPort();
        theListenerFactory = new ListenerFactory();
        theListenerFactory.setPort(thePort);
        theFtpServerFactory.addListener("default", theListenerFactory.createListener());

        // Create a UserManager instance
        PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
        File userFile = new File("users.properties");
        if (!userFile.exists()) {
            try {
                if (userFile.createNewFile()) {
                    System.out.println("File created: " + userFile.getName());
                } else {
                    System.out.println("File already exists.");
                }
            } catch (IOException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
            }
        }

        userManagerFactory.setFile(userFile); // Specify the file to store user details
        userManagerFactory.setPasswordEncryptor(new ClearTextPasswordEncryptor()); // Store plain text passwords
        UserManager myUserManager = userManagerFactory.createUserManager();
        // Create a user
        BaseUser myUser = new BaseUser();
        myUser.setName(aFtpCredential.getUsername());
        myUser.setPassword(aFtpCredential.getPassword());
        String myUsername = myUser.getName();
        theHomeDirectory = System.getProperty("java.io.tmpdir")  + String.format("/%s", myUsername);
        File directory = new File(theHomeDirectory); // Convert the string to a File object
        if (!directory.exists()) { // Check if the directory exists
            if (directory.mkdirs()) {
                System.out.println("Directory created: " + directory.getAbsolutePath());
            } else {
                System.out.println("Failed to create directory.");
            }
        }
        myUser.setHomeDirectory(theHomeDirectory);
        // Set write permissions for the user
        List<Authority> myAuthorities = new ArrayList<>();
        myAuthorities.add(new WritePermission());
        myUser.setAuthorities(myAuthorities);
        myUser.setHomeDirectory(theHomeDirectory);

        // Add the user to the user manager
        try {
            myUserManager.save(myUser);
        } catch (FtpException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // Set the user manager on the server context
        theFtpServerFactory.setUserManager(myUserManager);

        theFtpServer = theFtpServerFactory.createServer();
    }

    public String getHomeDirectory() {
        return theHomeDirectory;
    }

    public void run() {
        try {
            theFtpServer.start();
            System.out.println("FTP Server started on port " + thePort);
        } catch (FtpException aE) {
            // TODO Auto-generated catch block
            aE.printStackTrace();
        }
    }
}