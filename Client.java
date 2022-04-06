import java.io.IOException;
import java.net.Socket;
import java.util.*;

public class Client {
    int[] connectionPorts = {
            8000,
            8001,
            8002
    };
    String[] files;
    HashMap < Integer, ServerSocketConnection > serverConnections = new HashMap < > ();
    static final String host[] = {"dc01.utdallas.edu","dc02.utdallas.edu","dc21.utdallas.edu"};
    Random random = new Random();
    int clientID;

    public Client(int c) throws InterruptedException {
        clientID = c;
        connectServers();
        fetchFiles();
        Thread.sleep(200);
        execute();

    }

    public void connectServers() {
        for (int i = 0; i < connectionPorts.length; i++) {
            try {
                Socket s = new Socket(host[i], connectionPorts[i]);
                serverConnections.put(i, new ServerSocketConnection(s));

            } catch (IOException e) {
                System.out.println("Client couldn't connect to server");
                e.printStackTrace();
            }

        }

    }

    public void fetchFiles() {  // Function to ENQUIRE and fetch file details from Server
        int randomServer = 1; //random.nextInt(0,2)+0;
        try {
            serverConnections.get(randomServer).write(new ClientMessage(clientID, "Enquiry"));
            Object m = serverConnections.get(randomServer).read();
            if (m instanceof String && !((String) m).equalsIgnoreCase("Acknowledgement")) {
                files = ((String) m).split(",");
                System.out.println("received files " + m + " from server " + randomServer);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public synchronized void execute() {  // Function to send messages to the Servers to write data into a random file
        for (int i = 0; i < 5; i++) {
            int randomFile = random.nextInt(3);

            ClientMessage cm = new ClientMessage(clientID, randomFile, "Write", "" + System.currentTimeMillis() + " - " + clientID + " ");
            int randomServer = random.nextInt(3);
            try {
                serverConnections.get(randomServer).write(cm);
                System.out.println("Wrote message " + cm.message + " for file " + cm.fileNumber + " to server " + randomServer + " by client " + clientID);
            } catch (IOException e) {
                System.out.println("writing message failed at client");
            }
            try {
                Object m = serverConnections.get(randomServer).read();
                while (m instanceof String && !((String) m).equalsIgnoreCase("Acknowledgement")) {}
                System.out.println("received acknowledgement at client " + clientID + " for message " + cm.message);
            } catch (Exception e) {
                System.out.println("reading message failed at client");
            }
            try {
                Thread.sleep(random.nextInt(4) * 500);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        for (Integer i: serverConnections.keySet()) {
            try {
                serverConnections.get(i).write(new ClientMessage(clientID, "End"));
            } catch (IOException e) {
                System.out.println("client end message failed");
            }
        }

    }
}