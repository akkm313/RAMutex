import java.io.Serializable;

public class ServerMessage implements Serializable {
    int serverID;
    int fileNumber;
    int logicalClock;
    int clientID;
    String message;
    String type;
    ServerMessage(int s, int f, int l, String m, String t, int c) {
        clientID = c;
        serverID = s;
        fileNumber = f;
        logicalClock = l;
        message = m;
        type = t;
    }

}