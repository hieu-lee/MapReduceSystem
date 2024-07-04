package rs;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class CustomServer {
    private final CustomFTPServer theCustomFTPServer;
    private ServerSocket theServerSocket;
    private static final String THE_MAP_FILE_SUFFIX = "_map.txt";
    private static final String THE_REDUCE_FILE_SUFFIX = "_reduce.txt";
    private int theNumberOfServers;
    private String theServerName;
    private CustomFTPClient[] theMapFTPClients;
    private CustomFTPClient[] theReduceFTPClients;
    private int theIndex;
    private static final int MAX_FREE_BYTES = 1024 * 1024 * 50;

    public static void main(String[] args) {
        CustomFTPServer myCustomFTPServer = new CustomFTPServer(CustomFTPCredential.getInstance());
        CustomServer myCustomServer = new CustomServer(myCustomFTPServer);
        myCustomServer.run();
    }

    public CustomServer(CustomFTPServer aCustomFTPServer) {
        theCustomFTPServer = aCustomFTPServer;
        CustomFTPServerTask theCustomFTPServerTask = new CustomFTPServerTask(theCustomFTPServer);
        try {
            theServerSocket = new ServerSocket(SocketUtils.PORT);
        } catch (Exception aE) {
            aE.printStackTrace();
        }

        System.out.println("Socket server started on port " + SocketUtils.PORT);
        theCustomFTPServerTask.start();
    }

    private Stream<String> getWords(String aLine) {
        return Pattern.compile("\\P{L}+")
                .splitAsStream(aLine)
                .filter(word -> !word.isEmpty());
    }

    private boolean hasEnoughMemory(StringBuilder[] aTokensList) {
        long myPotentialMemory = Arrays.stream(aTokensList)
                .filter(aStringBuilder -> aStringBuilder != aTokensList[theIndex])
                .mapToInt(aStringBuilder -> aStringBuilder.length())
                .max()
                .orElse(0);
        return Runtime.getRuntime().freeMemory() + myPotentialMemory > MAX_FREE_BYTES;
    }


    private void getNumberOfServersAndServerName(Socket aClientSocket) {
        int myNumberOfServers = -1;
        try {
            String[] myMessage = SocketUtils.read(aClientSocket).split(" ", 2);
            myNumberOfServers = Integer.parseInt(myMessage[0]);
            theServerName = myMessage[1];
        } catch (Exception aE) {
            aE.printStackTrace();
        }
        theNumberOfServers = myNumberOfServers;
    }

    private String[] getServerNames() {
        String[] myServers = new String[theNumberOfServers];
        BufferedReader myReader;
        try {
            myReader = new BufferedReader(new FileReader(theCustomFTPServer.getHomeDirectory() + "/machines.txt"));
            for (int i = 0; i < theNumberOfServers; i++) {
                try {
                    myServers[i] = myReader.readLine().trim();
                    if (myServers[i] == null) {
                        myReader.close();
                        throw new RuntimeException("Not enough servers in machines.txt");
                    }
                    if (myServers[i].equals(theServerName)) {
                        theIndex = i;
                    }
                } catch (Exception aE) {
                    aE.printStackTrace();
                }
            }
        } catch (FileNotFoundException aE) {
            aE.printStackTrace();
        }
        theMapFTPClients = new CustomFTPClient[theNumberOfServers];
        theReduceFTPClients = new CustomFTPClient[theNumberOfServers];
        for (int i = 0; i < theNumberOfServers; i++) {
            if (!myServers[i].equals(theServerName)) {
                theMapFTPClients[i] = new CustomFTPClient(theServerName + THE_MAP_FILE_SUFFIX, myServers[i], CustomFTPCredential.getInstance());
                theMapFTPClients[i].setUpClient();
                theReduceFTPClients[i] = new CustomFTPClient(theServerName + THE_REDUCE_FILE_SUFFIX, myServers[i], CustomFTPCredential.getInstance());
                theReduceFTPClients[i].setUpClient();
            }
        }
        return myServers;
    }

    private StringBuilder mapAndShufflePhase1(String[] aServers, Socket aClientSocket) {
        StringBuilder[] myTokensList = new StringBuilder[theNumberOfServers];
        for (int i = 0; i < theNumberOfServers; i++) {
            myTokensList[i] = new StringBuilder();
        }
        Path filePath = Paths.get(theCustomFTPServer.getHomeDirectory(), "input.txt");
        CustomFTPClientTask[] myClientTasks = new CustomFTPClientTask[theNumberOfServers];
        for (int i = 0; i < theNumberOfServers; i++) {
            if (!theServerName.equals(aServers[i])) {
                myClientTasks[i] = new CustomFTPClientTask(new CustomFTPClient(theServerName + THE_MAP_FILE_SUFFIX, "", aServers[i], CustomFTPCredential.getInstance(), CustomFTPClientType.DELETE));
                myClientTasks[i].start();
            }
            else {
                myClientTasks[i] = null;
            }
        }
        for (int i = 0; i < theNumberOfServers; i++) {
            if (myClientTasks[i] != null) {
                try {
                    myClientTasks[i].join();
                } catch (InterruptedException aE) {
                    aE.printStackTrace();
                }
            }
        }
        try (BufferedReader myReader = Files.newBufferedReader(filePath)) {
            String myLine;
            while ((myLine = myReader.readLine()) != null) {
                getWords(myLine).forEach(aWord -> {
                    int myServerIndex = Math.abs(aWord.hashCode()) % theNumberOfServers;
                    myTokensList[myServerIndex].append(aWord).append(" ").append(1).append("\n");
                    if (!theServerName.equals(aServers[myServerIndex]) && !hasEnoughMemory(myTokensList)) {
                        int myLongestStringIndex = -1;
                        int myLongestStringLength = 0;
                        for (int i = 0; i < theNumberOfServers; i++) {
                            if (i != theIndex && myTokensList[i].length() > myLongestStringLength) {
                                myLongestStringIndex = i;
                                myLongestStringLength = myTokensList[i].length();
                            }
                        }
                        theMapFTPClients[myLongestStringIndex].appendFile(myTokensList[myLongestStringIndex]);
                        myTokensList[myLongestStringIndex] = new StringBuilder();
                    }
                });
            }
        } catch (Exception aE) {
            aE.printStackTrace();
        }
        List<Thread> myThreads = new ArrayList<>();
        for (int i = 0; i < theNumberOfServers; i++) {
            if (!theServerName.equals(aServers[i]) && myTokensList[i].length() > 0) {
                int j = i;
                Thread myThread = new Thread(() -> theMapFTPClients[j].appendFile(myTokensList[j].toString()));
                myThreads.add(myThread);
                myThread.start();
            }
        }
        myThreads.forEach(aThread -> {
            try {
                aThread.join();
            } catch (InterruptedException aE) {
                aE.printStackTrace();
            }
        });

        SocketUtils.write(aClientSocket, "map and shuffle 1 done");
        for (CustomFTPClient aFtpClient : theMapFTPClients) {
            if (aFtpClient != null) {
                aFtpClient.logoutAndDisconnect();
            }
        }
        System.out.println("map and shuffle 1 done");
        return myTokensList[theIndex];
    }

    private Map<String, Integer> reducePhase1(Socket aClientSocket, String[] aServers, StringBuilder aTokens) {
        if (SocketUtils.read(aClientSocket).equals("reduce 1 start")) {
            System.out.println("Start reduce phase 1");
        }
        else {
            try {
                theServerSocket.close();
            } catch (IOException aE) {
                aE.printStackTrace();
            }
            throw new RuntimeException("Invalid command");
        }

        Map<String, Integer> myWordCounts = new HashMap<>();

        IntStream.range(0, theNumberOfServers)
                .mapToObj(i -> !theServerName.equals(aServers[i])
                        ? readLinesFromFile(theCustomFTPServer.getHomeDirectory(), aServers[i])
                        : Arrays.stream(aTokens.toString().split("\n")))
                .flatMap(Function.identity())
                .filter(aLine -> !aLine.isEmpty())
                .map(aLine -> aLine.split(" "))
                .filter(aToken -> Math.abs(aToken[0].hashCode()) % theNumberOfServers == theIndex)
                .forEach(aToken -> myWordCounts.put(aToken[0], myWordCounts.getOrDefault(aToken[0], 0) + Integer.parseInt(aToken[1])));
        return myWordCounts;
    }

    private Stream<String> readLinesFromFile(String aDirectory, String aServerName) {
        try {
            return Files.lines(Paths.get(aDirectory, aServerName + THE_MAP_FILE_SUFFIX));
        } catch (IOException e) {
            e.printStackTrace();
            return Stream.empty();
        }
    }

    private int[] getBoundaries(Map<String, Integer> aWordCounts, Socket aClientSocket) {
        Map<Integer, Integer> myFreqCounts = new HashMap<>();
        for (Integer aCount : aWordCounts.values()) {
            myFreqCounts.merge(aCount, 1, Integer::sum);
        }
        String myLastReplyString = String.valueOf(myFreqCounts.size());
        for (Map.Entry<Integer, Integer> aEntry : myFreqCounts.entrySet()) {
            SocketUtils.write(aClientSocket, aEntry.getKey() + "-" + aEntry.getValue());
        }
        while (!SocketUtils.read(aClientSocket).equals(myLastReplyString)) {}
        SocketUtils.write(aClientSocket, "boundaries done");
        String myMessage = SocketUtils.read(aClientSocket);
        System.out.println("Boundaries done");
        return Arrays.stream(myMessage.split(" ")).mapToInt(Integer::parseInt).toArray();
    }


    private int getServerIndex(int aFreq, int[] aBoundaries) {
        int myServerIndex = 0;
        for (int i = 0; i < aBoundaries.length; i++) {
            if (aFreq <= aBoundaries[i]) {
                myServerIndex = i;
                break;
            }
        }
        return myServerIndex;
    }

    private StringBuilder shufflePhase2(Map<String, Integer> aWordCounts, String[] aServers, Socket aClientSocket, int[] aBoundaries) {
        StringBuilder[] myTokensList = new StringBuilder[theNumberOfServers];
        for (int i = 0; i < theNumberOfServers; i++) {
            myTokensList[i] = new StringBuilder();
        }

        CustomFTPClientTask[] myClientTasks = new CustomFTPClientTask[theNumberOfServers];
        for (int i = 0; i < theNumberOfServers; i++) {
            if (!theServerName.equals(aServers[i])) {
                myClientTasks[i] = new CustomFTPClientTask(new CustomFTPClient(theServerName + THE_REDUCE_FILE_SUFFIX, "", aServers[i], CustomFTPCredential.getInstance(), CustomFTPClientType.DELETE));
                myClientTasks[i].start();
            }
            else {
                myClientTasks[i] = null;
            }
        }
        for (int i = 0; i < theNumberOfServers; i++) {
            if (myClientTasks[i] != null) {
                try {
                    myClientTasks[i].join();
                } catch (InterruptedException aE) {
                    aE.printStackTrace();
                }
            }
        }

        for (Map.Entry<String, Integer> aEntry: aWordCounts.entrySet()) {
            int myServerIndex = getServerIndex(aEntry.getValue(), aBoundaries);
            myTokensList[myServerIndex].append(aEntry.getKey()).append(" ").append(aEntry.getValue()).append("\n");
            if (!theServerName.equals(aServers[myServerIndex]) && !hasEnoughMemory(myTokensList)) {
                int myLongestStringIndex = -1;
                int myLongestStringLength = 0;
                for (int i = 0; i < theNumberOfServers; i++) {
                    if (i != theIndex && myTokensList[i].length() > myLongestStringLength) {
                        myLongestStringIndex = i;
                        myLongestStringLength = myTokensList[i].length();
                    }
                }
                theReduceFTPClients[myLongestStringIndex].appendFile(myTokensList[myLongestStringIndex]);
                myTokensList[myLongestStringIndex] = new StringBuilder();
            }
        }
        List<Thread> myThreads = new ArrayList<>();
        for (int i = 0; i < theNumberOfServers; i++) {
            if (!theServerName.equals(aServers[i]) && myTokensList[i].length() > 0) {
                int j = i;
                Thread myThread = new Thread(() -> theReduceFTPClients[j].appendFile(myTokensList[j].toString()));
                myThreads.add(myThread);
                myThread.start();
            }
        }
        myThreads.forEach(aThread -> {
            try {
                aThread.join();
            } catch (InterruptedException aE) {
                aE.printStackTrace();
            }
        });
        SocketUtils.write(aClientSocket, "shuffle 2 done");
        System.out.println("Shuffle 2 done");
        return myTokensList[theIndex];
    }

    private void reducePhase2(Socket aClientSocket, StringBuilder aTokens, String[] aServerNames) {
        if (SocketUtils.read(aClientSocket).equals("reduce 2 start")) {
            System.out.println("Start reduce phase 2");
        } else {
            try {
                theServerSocket.close();
            } catch (IOException aE) {
                aE.printStackTrace();
            }
            throw new RuntimeException("Invalid command");
        }

        try (BufferedWriter myWriter = Files.newBufferedWriter(Paths.get(theCustomFTPServer.getHomeDirectory() + "/output.txt"))) {
            IntStream.range(0, theNumberOfServers)
                    .mapToObj(i -> {
                        try {
                            return !theServerName.equals(aServerNames[i]) ?
                                    Files.lines(Paths.get(theCustomFTPServer.getHomeDirectory() + "/" + aServerNames[i] + THE_REDUCE_FILE_SUFFIX))
                                            .flatMap(aLine -> Arrays.stream(aLine.split("\n")))
                                            .filter(aWord -> !aWord.isEmpty())
                                            .map(aWord -> {
                                                String[] aToken = aWord.split(" ");
                                                return new AbstractMap.SimpleEntry<>(Integer.parseInt(aToken[1]), aToken[0]);
                                            })
                                    : Arrays.stream(aTokens.toString().split("\n"))
                                    .filter(aWord -> !aWord.isEmpty())
                                    .map(aWord -> {
                                        String[] aToken = aWord.split(" ");
                                        return new AbstractMap.SimpleEntry<>(Integer.parseInt(aToken[1]), aToken[0]);
                                    });
                        } catch (IOException aE) {
                            aE.printStackTrace();
                            return Stream.<Map.Entry<Integer, String>>empty();
                        }
                    })
                    .flatMap(Function.identity())
                    .sorted(Comparator.comparingInt((Map.Entry<Integer, String> aEntry) -> aEntry.getKey())
                            .thenComparing(Map.Entry::getValue))
                    .forEachOrdered(aEntry -> {
                        try {
                            myWriter.write(aEntry.getKey() + " " + aEntry.getValue());
                            myWriter.newLine();
                        } catch (IOException aE) {
                            aE.printStackTrace();
                        }
                    });
            SocketUtils.write(aClientSocket, "reduce 2 done");
            for (CustomFTPClient aFTPClient : theReduceFTPClients) {
                if (aFTPClient != null) {
                    aFTPClient.logoutAndDisconnect();
                }
            }
            System.out.println("Reduce 2 done");
        } catch (IOException aE) {
            aE.printStackTrace();
        }
    }

    public void run() {
        Socket myClientSocket = null;
        try {
            myClientSocket = theServerSocket.accept();
        } catch (IOException aE) {
            aE.printStackTrace();
        }

        getNumberOfServersAndServerName(myClientSocket);
        if (theNumberOfServers == -1) {
            try {
                theServerSocket.close();
            } catch (IOException aE) {
                aE.printStackTrace();
            }
            System.out.println("Failed to get number of servers");
            return;
        }

        String[] myServers = getServerNames();

        StringBuilder myTokens = mapAndShufflePhase1(myServers, myClientSocket);

        Map<String, Integer> myWordCounts = reducePhase1(myClientSocket, myServers, myTokens);

        myTokens = shufflePhase2(myWordCounts, myServers, myClientSocket, getBoundaries(myWordCounts, myClientSocket));

        reducePhase2(myClientSocket, myTokens, myServers);

        try {
            assert myClientSocket != null;
            myClientSocket.close();
            theServerSocket.close();
        } catch (IOException aE) {
            aE.printStackTrace();
        }
    }
}
