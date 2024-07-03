package rs;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
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

    public static void main(String[] args) {
        CustomFTPServer myCustomFTPServer = new CustomFTPServer(CustomFTPCredential.getInstance());
        CustomServer myCustomServer = new CustomServer(myCustomFTPServer);
        myCustomServer.run();
    }

    public CustomServer(CustomFTPServer aCustomFTPServer) {
        theCustomFTPServer = aCustomFTPServer;
        try {
            theServerSocket = new ServerSocket(SocketUtils.PORT);
        } catch (Exception aE) {
            aE.printStackTrace();
        }

        System.out.println("Socket server started on port " + SocketUtils.PORT);
        theCustomFTPServer.start();
    }

    private Stream<String> getWords(String aLine) {
        return Pattern.compile("\\P{L}+")
                .splitAsStream(aLine)
                .filter(word -> !word.isEmpty());
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
                } catch (Exception aE) {
                    aE.printStackTrace();
                }
            }
        } catch (FileNotFoundException aE) {
            aE.printStackTrace();
        }
        return myServers;
    }

    private StringBuilder[] mapPhase1() {
        StringBuilder[] myTokens = new StringBuilder[theNumberOfServers];
        for (int i = 0; i < theNumberOfServers; i++) {
            myTokens[i] = new StringBuilder();
        }
        try {
            try (Stream<String> aLines = Files.lines(Paths.get(theCustomFTPServer.getHomeDirectory(), "input.txt"))) {
                aLines.flatMap(this::getWords)
                    .forEach(aWord -> {
                        int myServerIndex = (aWord.hashCode() % theNumberOfServers + theNumberOfServers) % theNumberOfServers;
                        myTokens[myServerIndex].append(aWord).append(" ").append(1).append("\n");
                    });
            }

        } catch (Exception aE) {
            aE.printStackTrace();
        }
        return myTokens;
    }

    private void shufflePhase1(String[] aServers, StringBuilder[] aTokens, Socket aClientSocket) {
        CustomFTPClient[] myClients = new CustomFTPClient[theNumberOfServers];
        for (int i = 0; i < theNumberOfServers; i++) {
            if (!theServerName.equals(aServers[i])) {
                myClients[i] = new CustomFTPClient(theServerName + THE_MAP_FILE_SUFFIX, aTokens[i].toString(), aServers[i], CustomFTPCredential.getInstance());
                myClients[i].start();
            }
        }
        for (int i = 0; i < theNumberOfServers; i++) {
            if (!theServerName.equals(aServers[i])) {
                try {
                    myClients[i].join();
                } catch (InterruptedException aE) {
                    aE.printStackTrace();
                }
            }
        }
        SocketUtils.write(aClientSocket, "map and shuffle 1 done");
        System.out.println("map and shuffle 1 done");
    }

    private Map<String, Integer> reducePhase1(Socket aClientSocket, String[] aServers, StringBuilder[] aTokensList) {
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
                        : Arrays.stream(aTokensList[i].toString().split("\n")))
                .flatMap(Function.identity())
                .filter(aLine -> !aLine.isEmpty())
                .map(aLine -> aLine.split(" ")).forEach(aTokens -> myWordCounts.put(aTokens[0], myWordCounts.getOrDefault(aTokens[0], 0) + Integer.parseInt(aTokens[1])));
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

    private Integer[] getBoundaries(Map<String, Integer> aWordCounts, Socket aClientSocket) {
        Map<Integer, Integer> myFreqCounts = new HashMap<>();
        for (Map.Entry<String, Integer> aEntry : aWordCounts.entrySet()) {
            myFreqCounts.put(aEntry.getValue(), myFreqCounts.getOrDefault(aEntry.getValue(), 0) + 1);
        }
        StringBuilder myFreqCountString = new StringBuilder();
        for (Map.Entry<Integer, Integer> aEntry : myFreqCounts.entrySet()) {
            myFreqCountString.append(aEntry.getKey()).append("-").append(aEntry.getValue()).append(" ");
        }
        SocketUtils.write(aClientSocket, myFreqCountString.toString().trim());
        String myMessage = SocketUtils.read(aClientSocket);
        List<Integer> myBoundaries = new ArrayList<>();
        System.out.println("Boundaries done");
        return Arrays.stream(myMessage.split(" ")).map(Integer::parseInt).toArray(Integer[]::new);
    }

    private int getServerIndex(int aFreq, Integer[] aBoundaries) {
        int myServerIndex = 0;
        for (int i = 0; i < aBoundaries.length; i++) {
            if (aFreq <= aBoundaries[i]) {
                myServerIndex = i;
                break;
            }
        }
        return myServerIndex;
    }

    private void shufflePhase2(Map<String, Integer> aWordCounts, String[] aServerNames, Socket aClientSocket, Integer[] aBoundaries, StringBuilder[] aTokensList) {
        CustomFTPClient[] myClients = new CustomFTPClient[theNumberOfServers];

        for (int i = 0; i < theNumberOfServers; i++) {
            aTokensList[i] = new StringBuilder();
        }

        for (Map.Entry<String, Integer> aEntry: aWordCounts.entrySet()) {
            int myServerIndex = getServerIndex(aEntry.getValue(), aBoundaries);
            aTokensList[myServerIndex].append(aEntry.getValue()).append(" ").append(aEntry.getKey()).append("\n");
        }

        for (int i = 0; i < theNumberOfServers; i++) {
            if (!theServerName.equals(aServerNames[i])) {
                myClients[i] = new CustomFTPClient(theServerName + THE_REDUCE_FILE_SUFFIX, aTokensList[i].toString(), aServerNames[i], CustomFTPCredential.getInstance());
                myClients[i].start();
            }
        }

        for (int i = 0; i < theNumberOfServers; i++) {
            if (!theServerName.equals(aServerNames[i])) {
                try {
                    myClients[i].join();
                } catch (InterruptedException aE) {
                    aE.printStackTrace();
                }
            }
        }
        SocketUtils.write(aClientSocket, "shuffle 2 done");
        System.out.println("Shuffle 2 done");
    }

    private void reducePhase2(Socket aClientSocket, StringBuilder[] aTokensList, String[] aServerNames) {
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
                                                String[] tokens = aWord.split(" ");
                                                return new AbstractMap.SimpleEntry<>(Integer.parseInt(tokens[0]), tokens[1]);
                                            })
                                    : Arrays.stream(aTokensList[i].toString().split("\n"))
                                    .filter(aWord -> !aWord.isEmpty())
                                    .map(aWord -> {
                                        String[] aTokens = aWord.split(" ");
                                        return new AbstractMap.SimpleEntry<>(Integer.parseInt(aTokens[0]), aTokens[1]);
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

        StringBuilder[] myTokensList = mapPhase1();

        shufflePhase1(myServers, myTokensList, myClientSocket);

        Map<String, Integer> myWordCounts = reducePhase1(myClientSocket, myServers, myTokensList);

        shufflePhase2(myWordCounts, myServers, myClientSocket, getBoundaries(myWordCounts, myClientSocket), myTokensList);

        reducePhase2(myClientSocket, myTokensList, myServers);

        try {
            assert myClientSocket != null;
            myClientSocket.close();
            theServerSocket.close();
        } catch (IOException aE) {
            aE.printStackTrace();
        }
    }
}
