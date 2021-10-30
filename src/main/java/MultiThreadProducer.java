public class MultiThreadProducer implements Runnable{
    String filepath;

    MultiThreadProducer(){
    }
    @Override
    public void run() {
        Main.throughputTest();
    }
}
