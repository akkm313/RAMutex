import java.io.*;
import java.net.Socket;

public class ServerSocketConnection {
    Socket s;
    ObjectOutputStream out;
    ObjectInputStream in ;
    boolean readDone = false;

    ServerSocketConnection(Socket sk) throws IOException {
        s = sk;
        out = new ObjectOutputStream(s.getOutputStream());

    }
    public Object read() throws IOException, ClassNotFoundException {
        if (!readDone)
            in = new ObjectInputStream(s.getInputStream());
        Object s = in .readObject();
        readDone = true;
        return s;

    }
    public void write(ClientMessage m) throws IOException {
        out.writeObject(m);
    }

}