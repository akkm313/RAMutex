import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.*;

public class Server {
    private static final String host[] = {"dc01.utdallas.edu","dc02.utdallas.edu","dc21.utdallas.edu"}; // host names
    private static final String critical = "Critical";    // Message Types - Critical , Request etc.
    private static final String request = "Request";
    private static final String write = "Write";
    private static final String enquiry = "Enquiry";
    private static final String reply = "Reply";
    public int[] serverPorts = {                         //Ports through which Servers will communicate with each other
            9000,
            9001,
            9002
    };
    public int[] clientPorts = {
            8000,
            8001,
            8002
    };
    public final int myID = 0;
    public final String myFolder = "Server" + myID + "/";  // folder from which Server will pick up the replicated file
    public final String files = "a.txt,b.txt,c.txt";
    public FileOutputStream[] fileOp = new FileOutputStream[3];
    public Integer logicalClock = 0;
    public Integer highestLogicalClock = 0;
    public Boolean[][] permissionRequired = new Boolean[3][3]; //[file][Server]
    public HashMap < Integer, SocketConnection > otherServers = new HashMap < > ();
    public HashMap < Integer, ClientSocketConnection > clients = new HashMap < > ();
    public boolean[] using = new boolean[3];
    public boolean[] waiting = new boolean[3];
    public int[][] outstandingReplies = new int[3][5];
    public List < List < ClientMessage >> deferredClients = Collections.synchronizedList(new ArrayList < List < ClientMessage >> ());
    public List < ServerMessage > deferredServerMessages = Collections.synchronizedList(new ArrayList < ServerMessage > ());
    boolean serversConnected = false;
    ServerSocket ss;
    public int noOfServerConnections = 0;

    public Server() throws InterruptedException, FileNotFoundException {
        initializeMemberValues();

        new Thread(new ServerListener()).start();  // Thread to listen for other Server Connections

        new Thread(new ClientListener()).start();  // Thread to listen to other client Connections

    }
    public void initializeMemberValues() throws FileNotFoundException {      // Function to initialize class variables
        Arrays.fill(permissionRequired[0], true);
        Arrays.fill(permissionRequired[1], true);
        Arrays.fill(permissionRequired[2], true);
        deferredClients.add(new ArrayList < ClientMessage > ());
        deferredClients.add(new ArrayList < ClientMessage > ());
        deferredClients.add(new ArrayList < ClientMessage > ());
        String[] fileNames = files.split(",");
        for (int i = 0; i < 3; i++)
            fileOp[i] = new FileOutputStream(myFolder + fileNames[i], true);
    }
    public void initializeOtherServers() {                             // Function to establish connections with other servers
        for (int i = 0; i < 3; i++) {
            if (i != myID) {
                try {
                    Socket s = new Socket(host[i], serverPorts[i]);

                    otherServers.put(i, new SocketConnection(s));

                } catch (Exception e) {
                    System.out.println("[ERROR]Unable to connect to Server " + i);
                }

            }

        }
        serversConnected = true;

    }

    class ServerListener implements Runnable {
        @Override
        public void run() {
            try {
                ss = new ServerSocket(serverPorts[myID]);
            } catch (IOException e) {
                System.out.println("[ERROR]Server socket error");
                System.exit(-1);
            }
            while (true) {
                System.out.println("Listening on server port");
                try {
                    Socket s = ss.accept();
                    System.out.println(" Server connection Accepted");
                    noOfServerConnections++;
                    new Thread(new connectedServer(s)).start();
                    if (noOfServerConnections == 2)
                        break;

                } catch (IOException e) {
                    System.out.println("[ERROR] Server socket connection error");
                }
            }
            try {
                ss.close();
                System.out.println("Server socket closed");
            } catch (IOException e) {
                System.out.println("[ERROR] Server socket closing failed");
            }

        }

    }
    public synchronized void processServerRequest(ServerMessage m) {    // Function to process a request from another Server
        System.out.println("Processing Server  REQUEST with message " + m.message + " to be written to file " + m.fileNumber);
        permissionRequired[m.fileNumber][m.serverID] = true;
        highestLogicalClock = Math.max(highestLogicalClock, m.logicalClock);
        // LOGIC for processing the request -Defer or Reply

        boolean defer = (waiting[m.fileNumber] || using[m.fileNumber]) && ((m.logicalClock > logicalClock) || (m.logicalClock == logicalClock && m.serverID > myID));
        System.out.println("Defer variable is " + defer + " Using: " + using[m.fileNumber] + " Waiting: " + waiting[m.fileNumber]);
        if (defer) {
            System.out.println("++++++++++++++++++++++++++++++++++++++++++++++");
            System.out.println("Deferred Server REQUEST with msg " + m.message + " for file " + m.fileNumber);
            System.out.println("+++++++++++++++++++++++++++++++++++++++++++++");
            deferredServerMessages.add(m);
        } else {
            logicalClock = highestLogicalClock + 1;
            ServerMessage repl = new ServerMessage(myID, m.fileNumber, logicalClock, m.message, reply, m.clientID);
            try {
                waiting[m.fileNumber] = true; //
                otherServers.get(m.serverID).sendMessage(repl);
                System.out.println("Sent REPLY to Server " + m.serverID + " for message " + m.message);
            } catch (Exception e) {
                System.out.println("[ERROR]Failed to send reply to Server " + m.serverID);
            }
        }

    }
    public synchronized void processServerReply(ServerMessage m) {   // Function to process a reply from another Server
        System.out.println("Processing REPLY from Server " + m.serverID + " with message- " + m.message);
        highestLogicalClock = Math.max(highestLogicalClock, m.logicalClock);
        int fileNumber = m.fileNumber;
        int senderID = m.serverID;
        permissionRequired[fileNumber][senderID] = false;
        //  System.out.println("Decrementing outstanding for file "+fileNumber+" client  "+m.clientID);
        outstandingReplies[fileNumber][m.clientID] -= 1;
        System.out.println("Pending messages for " + m.message + " to be written to file " + m.fileNumber + "is " + outstandingReplies[fileNumber][m.clientID]);
        if (outstandingReplies[fileNumber][m.clientID] == 0) {
            System.out.println("Pending replies for " + m.message + " is O. Entering CS ");
            enterCriticalSection(m.fileNumber, m.message, m.clientID);

        }
    }
    public void enterCriticalSection(int fileNumber, String message, int clientID) {   // Function to enter Critical section and Perform write on a replica
        sendCriticalSectionMessage(fileNumber, message, clientID);
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        System.out.println("CRITICAL SECTION with message " + message + " to be written to file " + fileNumber);
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        writeToFile(fileNumber, message);
        sendClientAck(clientID);
        criticalSectionCleanup(fileNumber, clientID);
        System.out.println("Critical section for " + message + " for file " + fileNumber + " DONE");
    }

    public void criticalSectionCleanup(int fileNumber, int clientID) {    // Cleaning up after a critical section execution, checking for deferred messages
        System.out.println("Cleaning up after Critical  Section for file " + fileNumber);
        using[fileNumber] = false;
        waiting[fileNumber] = false; //
        System.out.println("Going through Deferred Server messages");
        ArrayList < ServerMessage > toBeDeleted = new ArrayList < > ();
        for (ServerMessage msg: deferredServerMessages) {
            if (msg.fileNumber == fileNumber) {
                ServerMessage rep = new ServerMessage(myID, msg.fileNumber, logicalClock, msg.message, reply, msg.clientID);
                try {
                    toBeDeleted.add(msg);
                    System.out.println("Reply for Deferred Message : " + msg.message + " is being sent to Server " + msg.serverID);
                    otherServers.get(msg.serverID).sendMessage(rep);
                    permissionRequired[fileNumber][msg.serverID] = true;
                } catch (Exception e) {
                    System.out.println("[ERROR]Failed to send reply message to Server " + msg.serverID);
                }
            }

        }
        boolean flag = false;
        if (toBeDeleted.size() == 0)
            flag = true;
        for (ServerMessage m: toBeDeleted)
            deferredServerMessages.remove(m);
        System.out.println("Going through deferred client messages");
        if (flag && deferredClients.get(fileNumber).size() > 0) {
            System.out.println(" Found a deferred client message, direct critical section access ");
            directCriticalSection(getDeferredClientMessage(fileNumber));
        } else {
            if (deferredClients.get(fileNumber).size() > 0) {
                System.out.println(" Found a deferred client message, sending request to other servers");
                sendRequest(getDeferredClientMessage(fileNumber));
            }

        }
        System.out.println("Critical section Cleanup over");

    }
    public synchronized ClientMessage getDeferredClientMessage(int f) {
        ClientMessage c = deferredClients.get(f).get(0);
        deferredClients.get(f).remove(0);
        return c;
    }

    class connectedServer implements Runnable {    // Thread to handle a Server socket connection
        Socket cs;
        ObjectInputStream in ;
        connectedServer(Socket s) {
            cs = s;
        }
        @Override
        public void run() {
            try {
                in = new ObjectInputStream(cs.getInputStream());
                while (true) {
                    ServerMessage m = (ServerMessage) in .readObject();

                    if (m.type.equalsIgnoreCase(request))
                        processServerRequest(m);
                    else if (m.type.equalsIgnoreCase(reply)) {
                        processServerReply(m);
                    } else if (m.type.equalsIgnoreCase(critical)) {
                        processServerCritical(m);
                    }

                }

            } catch (Exception e) {
                System.out.println("Closing the Server Connections.. ");
                try {
                    cs.close();
                } catch (IOException ex) {
                    System.out.println("[ERROR] Closing the server connection failed");
                }
                System.exit(-1);
            }
        }
    }
    public synchronized void processServerCritical(ServerMessage m) {   // Process a message of Type 'Critical' from a neighboring Server
        using[m.fileNumber] = true;
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        System.out.println("Received CRITICAL message from Server " + m.serverID + " Entering CRITICAL SECTION with message " + m.message);
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        writeToFile(m.fileNumber, m.message);
        // sendClientAck(m.clientID);
        using[m.fileNumber] = false;
        waiting[m.fileNumber] = false;
        criticalSectionCleanup(m.fileNumber, m.clientID);
        System.out.println("Completed CRITICAL SECTION for message " + m.message + " for file " + m.fileNumber);
    }

    public synchronized void writeToFile(int fileNumber, String message) {  // Function to write data into my copy of the file
        System.out.println("Writing to File " + fileNumber);
        try {
            fileOp[fileNumber].write(message.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            System.out.println("[ERROR] Writing to file "+fileNumber+" failed");
        }
    }

    public synchronized void sendRequest(ClientMessage cm) {  // Function to send Request to other Servers to get access to the Critical section
        System.out.println("Send REQUEST with message " + cm.message + " for file " + cm.fileNumber + " to other servers");
        logicalClock = highestLogicalClock + 1;
        boolean flag = false;
        ServerMessage sm = new ServerMessage(myID, cm.fileNumber, logicalClock, cm.message, request, cm.clientID);

        waiting[cm.fileNumber] = true;

        for (int i = 0; i < 3; i++) {
            if (i != myID && permissionRequired[sm.fileNumber][i]) {
                flag = true;
                try {
                    outstandingReplies[sm.fileNumber][sm.clientID] += 1;
                    System.out.println("Incremented pending replies for " + cm.message + " fo file " + cm.fileNumber + " to " + outstandingReplies[sm.fileNumber][sm.clientID]);
                    otherServers.get(i).sendMessage(sm);

                } catch (Exception e) {
                    System.out.println("[ERROR] Failed to send request to Server " + i);
                    System.exit(-1);
                }
            }
        }
        if (!flag) {
            System.out.println("No permissions required , can access CRITICAL SECTION");
            directCriticalSection(cm);

        }
        System.out.println("Completed sending REQUEST messages to other servers");

    }

    public void directCriticalSection(ClientMessage cm) {  // Function to directly access the CS in case no permissions are Required ( Roucairol and Carvalho) Optimization
        System.out.println("Direct access to Critical Section");
        using[cm.fileNumber] = true;
        sendCriticalSectionMessage(cm.fileNumber, cm.message, cm.clientID);
        System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        System.out.println("DIRECT CRITICAL SECTION with message" + cm.message + " for file " + cm.fileNumber);
        System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
        writeToFile(cm.fileNumber, cm.message);

        sendClientAck(cm.clientID);
        criticalSectionCleanup(cm.fileNumber, cm.clientID);
        System.out.println("Completed direct CRITICAL SECTION Execution");
        //        using[cm.fileNumber]=false;
        //        waiting[cm.fileNumber]=false;

    }
    public synchronized void sendClientAck(int clientID) {  // Function to send an Acknowledgment to Client after CS Execution
        System.out.println("Sending ack to Client" + clientID);
        try {
            System.out.println("clients" + clientID);
            clients.get(Integer.valueOf(clientID)).sendAcknowledgement();
        } catch (IOException e) {
            System.out.println("[ERROR]Failed to send acknowledgment to client "+clientID);
        }
    }

    public synchronized void sendCriticalSectionMessage(int fileNumber, String message, int clientID) {  // Function to send message of type 'Critical' to other servers
        System.out.println("Sending CRITICAL message to other servers for message " + message + " to be written to " + fileNumber);
        ServerMessage sm = new ServerMessage(myID, fileNumber, logicalClock, message, critical, clientID);
        for (int i = 0; i < 3; i++) {
            if (i != myID) {
                try {
                    otherServers.get(i).sendMessage(sm);
                } catch (Exception e) {
                    System.out.println("[ERROR]Failed to send  CRITICAL message to Server " + i);
                }
            }
        }
        System.out.println("Completed sending CRITICAL messages to other servers");

    }
    class ClientListener implements Runnable {  // thread that listens for Client connections

        @Override
        public void run() {
            ServerSocket clientListener = null;
            try {
                clientListener = new ServerSocket(clientPorts[myID]);
            } catch (Exception e) {
                System.out.println("[ERROR]Client listener failed");
            }
            while (true) {
                try {
                    Socket s = clientListener.accept();
                    System.out.println("client accepted");
                    if (!serversConnected)
                        initializeOtherServers();
                    new Thread(new connectedClient(s)).start();
                } catch (Exception e) {
                    System.out.println("[ERROR]Client socket accepting failed");
                }

            }

        }

    }

    class connectedClient implements Runnable {  //thread that handles a connected client
        Socket s;
        ObjectInputStream in ;
        boolean endReceived;
        connectedClient(Socket socket) throws IOException {
            s = socket; in = new ObjectInputStream(s.getInputStream());

        }
        @Override
        public void run() {
            endReceived = false;
            while (!endReceived) {
                try {
                    ClientMessage cm = (ClientMessage) in .readObject();
                    System.out.println("Received CLIENT message " + cm.message + "from client " + cm.clientID);
                    int clientID = cm.clientID;
                    if (!clients.containsKey(clientID)) { //  System.out.println("Adding Client"+clientID);
                        clients.put(clientID, new ClientSocketConnection(s));
                    }
                    if (cm.type.equalsIgnoreCase(write)) {
                        int fileNumber = cm.fileNumber;
                        if (using[fileNumber]) {
                            System.out.println("------------------------------------");
                            System.out.println("Deferred Client Message " + cm.message);
                            System.out.println("-----------------------------------");
                            addToDeferredClients(cm);
                        } else {
                            sendRequest(cm);
                        }

                    } else if (cm.type.equalsIgnoreCase(enquiry)) {
                        while (!clients.containsKey(clientID)) {}
                        clients.get(clientID).sendEnquiryReply(files);

                    } else if (cm.type.equalsIgnoreCase("End")) {
                        endReceived = true;
                    }

                } catch (Exception e) {
                    System.out.println("[ERROR]Reading from client failed");
                    e.printStackTrace();
                    break;
                    //  System.exit(-1);

                } finally {
                    try {
                        if (endReceived)
                            s.close();
                    } catch (Exception e) {
                        System.out.println("[ERROR] Closing the client socket failed");

                    }
                }

            }

        }
    }
    public synchronized void addToDeferredClients(ClientMessage cm) {
        deferredClients.get(cm.fileNumber).add(cm);
    }

    public static void main(String[] args) throws InterruptedException, FileNotFoundException {
        Server s = new Server();
    }

}