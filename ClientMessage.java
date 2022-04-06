import java.io.Serializable;

public class ClientMessage implements Serializable {
    int clientID;
    int fileNumber;
    String type;
    String message = "";

    ClientMessage(int c, int f, String t, String m) {
        clientID = c;
        fileNumber = f;
        type = t;
        message = m;
    }
    ClientMessage(int c, String t) {
        clientID = c;
        type = t;
    }

}