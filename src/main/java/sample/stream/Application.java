package sample.stream;

import akka.actor.*;
import akka.japi.pf.ReceiveBuilder;
import akka.routing.ActorRefRoutee;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Routee;
import akka.routing.Router;
import akka.stream.ActorMaterializer;
import akka.stream.actor.AbstractActorSubscriber;
import akka.stream.actor.ActorSubscriberMessage;
import akka.stream.actor.MaxInFlightRequestStrategy;
import akka.stream.actor.RequestStrategy;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Sink;
import akka.util.ByteIterator;
import akka.util.ByteString;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

//import scala.tools.cmd.gen.AnyVals;


class WorkerPoolProtocol {

    public static class Msg {
        public final ByteString data;
        public  final ActorRef replyTo;

        public Msg(ByteString data, ActorRef replyTo) {
            this.data = data;
            this.replyTo = replyTo;
        }

        @Override
        public String toString() {
            return String.format("Msg(%s, %s)", data, replyTo);
        }
    }

    public static Msg msg(ByteString data, ActorRef replyTo) {
        return new Msg(data, replyTo);
    }

    public static class Work {
        public final ByteString data;
        /*public ByteString result;*/
        public Work(ByteString data) { this.data = data; /*this.result = null;*/ }


        @Override
        public String toString() {
            return String.format("Work(%s)", data);
        }
    }
    public static Work work(ByteString data) {
        return new Work(data);
    }


    public static class Reply {
        public final ByteString data;
        public Reply(ByteString data) { this.data = data; }

        @Override
        public String toString() {
            return String.format("Reply(%s)", data);
        }
    }
    public static Reply reply(ByteString data) {
        return new Reply(data);
    }


    public static class Done {
        public final ByteString data;
        public Done(ByteString data) { this.data = data; }

        @Override
        public String toString() {
            return String.format("Done(%s)", data);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Done done = (Done) o;

            if (data != done.data) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            return data.length();
        }
    }
    public static Done done(ByteString data) {
        return new Done(data);
    }

}

 class WorkerPool extends AbstractActorSubscriber {

    public static Props props() { return Props.create(WorkerPool.class); }

    final int MAX_QUEUE_SIZE = 10;
    final Map<ByteString, ActorRef> queue = new HashMap<>();

    final Router router;

    @Override
    public RequestStrategy requestStrategy() {
        return new MaxInFlightRequestStrategy(MAX_QUEUE_SIZE) {
            @Override
            public int inFlightInternally() {
                return queue.size();
            }
        };
    }

    public WorkerPool() {
        final List<Routee> routees = new ArrayList<>();
        int workerCount = Runtime.getRuntime().availableProcessors();
        for (int i = 0; i < workerCount*2; i++)
            routees.add(new ActorRefRoutee(context().actorOf(Props.create(Worker.class, i))));
        router = new Router(new RoundRobinRoutingLogic(), routees);

        receive(ReceiveBuilder.
                match(ActorSubscriberMessage.OnNext.class, on -> on.element() instanceof WorkerPoolProtocol.Msg,
                        onNext -> {
                            WorkerPoolProtocol.Msg msg = (WorkerPoolProtocol.Msg) onNext.element();
                            queue.put(msg.data, msg.replyTo);

                            if (queue.size() > MAX_QUEUE_SIZE)
                                throw new RuntimeException("queued too many: " + queue.size());

                            router.route(WorkerPoolProtocol.work(msg.data), self());
                        }).
                match(WorkerPoolProtocol.Reply.class, reply -> {
                    ByteString data = reply.data;
                    queue.get(data).tell(WorkerPoolProtocol.done(data), self());
                    queue.remove(data);
                }).
                build());
    }
}

class Worker extends AbstractActor {
    public static int byteArrayToLeInt(byte[] encodedValue) {
        int value = (encodedValue[3] << (Byte.SIZE * 3));
        value |= (encodedValue[2] & 0xFF) << (Byte.SIZE * 2);
        value |= (encodedValue[1] & 0xFF) << (Byte.SIZE * 1);
        value |= (encodedValue[0] & 0xFF);
        return value;
    }

    public static int byteArrayToLeShort(byte[] encodedValue) {
        int value = ((encodedValue[0]& 0xFF) << (Byte.SIZE * 1));
        value |= (encodedValue[1] & 0xFF);
        return value;
    }

    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();

    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    public Worker(int nodeId) {
        //System.out.println(self());
        receive(ReceiveBuilder.
                match(WorkerPoolProtocol.Work.class, work -> {
                    ByteIterator it = work.data.iterator();
                    ByteIterator itCopy;
                    it.getLong(ByteOrder.nativeOrder());
                    it.getLong(ByteOrder.nativeOrder());
                    it.getLong(ByteOrder.nativeOrder());
                    int bytesSkip = 0;
                    long timestamp = 0;
                    byte[] date = new byte[4];
                    //int date = 0;
                    while(it.hasNext()){
                        itCopy = it.clone();
                        itCopy.getInt(ByteOrder.BIG_ENDIAN);
                        itCopy.getBytes(date);
                        //timestamp = itCopy.getLong(ByteOrder.BIG_ENDIAN);

                        if (itCopy.getInt(ByteOrder.nativeOrder())==itCopy.getInt(ByteOrder.nativeOrder())) {
                            break;
                        }
                        else
                            it.next();
                            bytesSkip ++;
                    }
                    System.out.println(new Date(byteArrayToLeInt(date)));
                    System.out.println(bytesSkip);
                    Path path = Paths.get("/tmp/" + nodeId);
                    //if directory exists?
                    if (!Files.exists(path)) {
                        try {
                            Files.createDirectories(path);
                        } catch (IOException e) {
                            //fail to create directory
                            e.printStackTrace();
                        }
                    }
                    Files.newByteChannel(Paths.get("/tmp/"+ nodeId +"/" + work.data.hashCode()),CREATE, WRITE ).write(work.data.toByteBuffer());
                    //Files.write(Paths.get("/tmp/"+ nodeId +"/" + work.data.hashCode()), work.data.toByteBuffer());
                    sender().tell(WorkerPoolProtocol.reply(work.data), self());
                }).build());
    }
}


class ClientActor extends UntypedActor {
    @Override
    public void onReceive(Object message) throws Exception {
       // System.out.println(sender());
    }
}

class DatabaseActor extends UntypedActor {
    @Override
    public void onReceive(Object message) throws Exception {
        MongoClient mongo = new MongoClient( "localhost" , 27017 );
        DB db = mongo.getDB("akka");
        BasicDBObject document = new BasicDBObject();
        document.put("filename", "ipp.pcap");
        document.put("timestamp", new Date());
        document.put("actorRef", 1);
        document.put("bytesSkip", 50);
        DBCollection table = db.getCollection("test");
        table.insert(document);
        // System.out.println(sender());
    }
}

public class Application {
  public static void main(String[] args) throws IOException {
    final ActorSystem system = ActorSystem.create("Sys");
    final ActorMaterializer materializer = ActorMaterializer.create(system);
    final Integer chunkSize = 1024 * 100; //bytes
    final String inPath = "/home/laboshinl/workspace/akka-pcap/src/main/resources/ipp.pcap";
    final File inputFile = new File(inPath);

    ActorRef replyTo = system.actorOf(Props.create(DatabaseActor.class));

    FileIO.fromFile(inputFile, chunkSize)
            .map(i -> WorkerPoolProtocol.msg(i, replyTo))
            .runWith(Sink.<WorkerPoolProtocol.Msg>actorSubscriber(WorkerPool.props()), materializer);
  }

}
