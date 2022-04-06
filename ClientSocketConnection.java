import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class ClientSocketConnection {
    Socket s;
    ObjectOutputStream out;
    ClientSocketConnection(Socket client) throws IOException {
        s = client;
        out = new ObjectOutputStream(s.getOutputStream());
    }
    public void sendEnquiryReply(String fileList) {
        try {
            out.writeObject(fileList);
        } catch (Exception e) {
            System.out.println("enquiry reply failed");
        }

    }

    public void sendAcknowledgement() throws IOException {
        out.writeObject(new String("Acknowledgement"));
    }
}