import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class SocketConnection {
    Socket connection;
    ObjectOutputStream out;
    ObjectInputStream in ;

    SocketConnection(Socket s) {
        connection = s;
        try {
            out = new ObjectOutputStream(s.getOutputStream());

        } catch (Exception e) {
            System.out.println("Error initializing Socket connection");
        }

    }
    public void sendMessage(ServerMessage m) {
        try {
            out.writeObject(m);
        } catch (Exception e) {
            System.out.println("couldnt send msg to server " + m.serverID);
            e.printStackTrace();

        }
    }

}