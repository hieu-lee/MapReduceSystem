package rs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class CustomClient {
    private final List<String> theMachinesList;
    private final int theNumberOfServers;
    private final List<String> theInputFilenames;
    private static final String THE_INPUT_FILES_DIRECTORY = "input_files";

    public static void main(String[] args) {
        long myStartTime = System.currentTimeMillis();;
        CustomClient myClient = new CustomClient();
        myClient.run();
        long myEndTime = System.currentTimeMillis();;
        System.out.println("Time taken: " + (myEndTime - myStartTime) + "ms");
    }

    public CustomClient() {
        theMachinesList = new ArrayList<>();
        try {
            theMachinesList.addAll(Files.readAllLines(Paths.get("./machines.txt")));
        } catch (IOException aE) {
            aE.printStackTrace();
        }
        theNumberOfServers = theMachinesList.size();
        try {
            Files.deleteIfExists(Paths.get("output.txt"));
        } catch (IOException aE) {
            aE.printStackTrace();
        }

        File myFolder = new File(THE_INPUT_FILES_DIRECTORY);
        theInputFilenames = Arrays.stream(Objects.requireNonNull(myFolder.listFiles()))
                .filter(File::isFile)
                .map(File::getName)
                .collect(Collectors.toList());
    }

    private void publishFileContents() {
        ExecutorService myExecutorService = Executors.newFixedThreadPool(theNumberOfServers);
        BufferedWriter[] myBufferedWriters = new BufferedWriter[theNumberOfServers];

        try {
            // Initialize BufferedWriter objects in parallel
            IntStream.range(0, theNumberOfServers).parallel().forEach(i -> {
                try {
                    Path path = Paths.get("input" + i + ".txt");
                    if (!Files.exists(path)) {
                        Files.createFile(path);
                    }
                    myBufferedWriters[i] = Files.newBufferedWriter(path, StandardOpenOption.APPEND);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            // Process files in parallel
            for (String aFilename : theInputFilenames) {
                File myFile = new File(THE_INPUT_FILES_DIRECTORY + "/" + aFilename);
                AtomicInteger myCount = new AtomicInteger(0);
                try (Stream<String> myLinesStream = Files.lines(myFile.toPath()).parallel()) {
                    myLinesStream.forEach(aLine -> {
                        int myServerIndex = myCount.getAndIncrement() % theNumberOfServers;
                        try {
                            myBufferedWriters[myServerIndex].write(aLine.toLowerCase() + "\n");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } finally {
            // Close BufferedWriter objects
            IntStream.range(0, theNumberOfServers).parallel().forEach(i -> {
                try {
                    if (myBufferedWriters[i] != null) {
                        myBufferedWriters[i].close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        // FTP client tasks
        CustomFTPClientTask[] theFTPClients = new CustomFTPClientTask[theNumberOfServers];
        for (int i = 0; i < theNumberOfServers; i++) {
            theFTPClients[i] = new CustomFTPClientTask(new CustomFTPClient("input.txt", Paths.get("input" + i + ".txt"), theMachinesList.get(i), CustomFTPCredential.getInstance(), CustomFTPClientType.STORE));
            myExecutorService.submit(theFTPClients[i]);
        }
        myExecutorService.shutdown();
        try {
            myExecutorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Delete files
        IntStream.range(0, theNumberOfServers).parallel().forEach(i -> {
            try {
                Files.deleteIfExists(Paths.get("input" + i + ".txt"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }


    private void publishServerNames() {
        CustomFTPClientTask[] theServerNamesFTPClients = new CustomFTPClientTask[theNumberOfServers];
        String myServerNames = String.join("\n", theMachinesList);
        for (int i = 0; i < theNumberOfServers; i++) {
            String theServerName = theMachinesList.get(i).trim();
            theServerNamesFTPClients[i] = new CustomFTPClientTask(new CustomFTPClient("machines.txt", myServerNames, theServerName, CustomFTPCredential.getInstance()));
            theServerNamesFTPClients[i].start();
        }
        for (int i = 0; i < theNumberOfServers; i++) {
            try {
                theServerNamesFTPClients[i].join();
            } catch (InterruptedException aE) {
                aE.printStackTrace();
            }
        }
    }

    private Socket[] createClientSockets() {
        Socket[] theClientSockets = new Socket[theNumberOfServers];
        for (int i = 0; i < theNumberOfServers; i++) {
            try {
                String myMachine = theMachinesList.get(i).trim();
                theClientSockets[i] = new Socket(myMachine, SocketUtils.PORT);
            } catch (IOException aE) {
                aE.printStackTrace();
            }
        }
        return theClientSockets;
    }

    private void publishNumberOfServersAndServerNames(Socket[] aClientSockets) {
        for (int i = 0; i < theNumberOfServers; i++) {
            SocketUtils.write(aClientSockets[i], theNumberOfServers + " " + theMachinesList.get(i));
        }
    }

    private void checkMessageFromAllServers(Socket[] aClientSockets, String aMessage) {
        for (int i = 0; i < theNumberOfServers; i++) {
            String myOutput = SocketUtils.read(aClientSockets[i]);
            if (!myOutput.equals(aMessage)) {
                throw new RuntimeException(aMessage + " failed");
            }
        }
    }

    private void mapPhase1(Socket[] aClientSockets) {
        publishNumberOfServersAndServerNames(aClientSockets);
        checkMessageFromAllServers(aClientSockets, "map and shuffle 1 done");
    }

    private void reducePhase1(Socket[] aClientSockets) {
        writeToAllServers(aClientSockets, "reduce 1 start");
    }

    private void writeToAllServers(Socket[] aClientSockets, String aMessage) {
        for (int i = 0; i < theNumberOfServers; i++) {
            SocketUtils.write(aClientSockets[i], aMessage);
        }
    }

    private List<Integer> getBoundaries(Socket[] aClientSockets) {
        Map<Integer, Integer> myWordFreqCounter = new HashMap<>();
        for (int i = 0; i < theNumberOfServers; i++) {
            int myCount = 0;
            String myMessage = SocketUtils.read(aClientSockets[i]);
            while (!myMessage.equals("boundaries done")) {
                myCount++;
                String[] myEntryString = myMessage.split("-");
                int myKey = Integer.parseInt(myEntryString[0]);
                int myValue = Integer.parseInt(myEntryString[1]);
                myWordFreqCounter.put(myKey, myWordFreqCounter.getOrDefault(myKey, 0) + myValue);
                SocketUtils.write(aClientSockets[i], String.valueOf(myCount));
                myMessage = SocketUtils.read(aClientSockets[i]);
            }
        }
        int myTotalFreqCount = myWordFreqCounter.values().stream().mapToInt(i -> i).sum();
        List<FreqCount> myFreqCountList = new ArrayList<>();
        for (Map.Entry<Integer, Integer> myEntry : myWordFreqCounter.entrySet()) {
            myFreqCountList.add(new FreqCount(myEntry.getKey(), myEntry.getValue()));
        }
        myFreqCountList.sort(Comparator.comparingInt(FreqCount::getFreq));
        int myCurrentFreqCount = 0;
        List<Integer> myBoundaries = new ArrayList<>();
        int n = myFreqCountList.size();
        for (int i = 0; i < myFreqCountList.size(); i++) {
            if (myBoundaries.size() == theNumberOfServers - 1) {
                myBoundaries.add(myFreqCountList.get(myFreqCountList.size() - 1).getFreq());
                break;
            }
            int myFreqCount = myFreqCountList.get(i).getCount();
            myCurrentFreqCount += myFreqCount;
            if (myCurrentFreqCount >= ((double)myTotalFreqCount / (double)n)) {
                if (myCurrentFreqCount - myFreqCount == 0) {
                    myBoundaries.add(myFreqCountList.get(i).getFreq());
                    n -= (i + 1);
                    myTotalFreqCount -= myCurrentFreqCount;
                    myCurrentFreqCount = 0;
                }
                else {
                    myBoundaries.add(myFreqCountList.get(i - 1).getFreq());
                    n -= i;
                    myTotalFreqCount -= (myCurrentFreqCount - myFreqCount);
                    myCurrentFreqCount = myFreqCount;
                }
            }
        }
        return myBoundaries;
    }

    private void publishBoundaries(Socket[] aClientSockets, List<Integer> aBoundaries) {
        writeToAllServers(aClientSockets, aBoundaries.stream().map(Object::toString).collect(Collectors.joining(" ")));
    }

    private void awaitShufflePhase2(Socket[] aClientSockets) {
        checkMessageFromAllServers(aClientSockets, "shuffle 2 done");
    }

    private void reducePhase2(Socket[] aClientSockets) {
        writeToAllServers(aClientSockets, "reduce 2 start");
    }

    private void awaitReducePhase2(Socket[] aClientSockets) {
        CustomFTPClient[] myResultFTPClients = new CustomFTPClient[theNumberOfServers];
        checkMessageFromAllServers(aClientSockets, "reduce 2 done");
        for (int i = 0; i < theNumberOfServers; i++) {
            myResultFTPClients[i] = new CustomFTPClient("output.txt", "", theMachinesList.get(i).trim(), CustomFTPCredential.getInstance(), CustomFTPClientType.DISPLAY);
            myResultFTPClients[i].run();
        }
        System.out.println("Output file generated");
    }

    private void closeClientSockets(Socket[] aClientSockets) {
        for (int i = 0; i < theNumberOfServers; i++) {
            try {
                aClientSockets[i].close();
            } catch (IOException aE) {
                aE.printStackTrace();
            }
        }
    }

    public void run() {
        publishFileContents();

        publishServerNames();

        Socket[] myClientSockets = createClientSockets();

        mapPhase1(myClientSockets);

        reducePhase1(myClientSockets);

        publishBoundaries(myClientSockets, getBoundaries(myClientSockets));

        awaitShufflePhase2(myClientSockets);

        reducePhase2(myClientSockets);

        awaitReducePhase2(myClientSockets);

        closeClientSockets(myClientSockets);
    }
}
