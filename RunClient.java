public class RunClient {

    public static void main(String[] args) {
        new Thread(new clientThread(1)).start();
        new Thread(new clientThread(2)).start();
        new Thread(new clientThread(3)).start();
        new Thread(new clientThread(4)).start();

    }

    static class clientThread implements Runnable {
        int ID;
        clientThread(int id) {
            ID = id;
        }
        @Override
        public void run() {
            try {
                new Client(ID);
            } catch (InterruptedException e) {
                System.out.println("Client creating ERROR");
            }

        }
    }
}