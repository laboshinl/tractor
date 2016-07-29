package ru.laboshinl.tractor;

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
import akka.stream.javadsl.Source;
import akka.util.ByteIterator;
import akka.util.ByteString;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

import com.mongodb.*;
import org.bitbucket.dollar.Dollar;

//import scala.tools.cmd.gen.AnyVals;


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
        //final List<ActorRef> refs = new ArrayList<>();

        int workerCount = Runtime.getRuntime().availableProcessors();

        ActorRef [] refs = new ActorRef[workerCount*2];
        for (int i = 0; i < workerCount*2; i++) {
            ActorRef ref = getContext().actorOf(Props.create(Worker.class, i));
            routees.add(new ActorRefRoutee(ref));
            refs[i] = ref;
        }

        router = new Router(new RoundRobinRoutingLogic(), routees);
        ActorRef databaseActor = context().actorOf(Props.create(DatabaseActor.class));

        receive(ReceiveBuilder.
                match(ActorSubscriberMessage.OnNext.class, on -> on.element() instanceof WorkerPoolProtocol.Msg,
                        onNext -> {
                            WorkerPoolProtocol.Msg msg = (WorkerPoolProtocol.Msg) onNext.element();
//                            queue.put(msg.data, msg.replyTo);
//
//                            if (queue.size() > MAX_QUEUE_SIZE)
//                                throw new RuntimeException("queued too many: " + queue.size());

                            router.route(WorkerPoolProtocol.work(msg.data), self());
                        }).
                match(WorkerPoolProtocol.Database_msg.class, database_msg -> {
                    databaseActor.tell(database_msg, self());
                }).
                match(ActorSubscriberMessage.OnNext.class, on -> on.element() instanceof WorkerPoolProtocol.ChunkPath,
                onNext -> {
                    WorkerPoolProtocol.ChunkPath filename = (WorkerPoolProtocol.ChunkPath) onNext.element();
                    refs[(int) filename.nodeid].tell(filename.chunkname, self());
                    System.out.println(filename.nodeid);


                }).
//                match(WorkerPoolProtocol.Reply.class, reply -> {
//                    ByteString data = reply.data;
//                    queue.get(data).tell(WorkerPoolProtocol.done(data), self());
//                    queue.remove(data);
//                }).
        build());
    }
}

class Worker extends AbstractActor {
    final List<Integer> validEthertypes =
            Dollar.$(Integer.parseInt("800", 16), Integer.parseInt("808", 16))
                    .concat(Dollar.$(Integer.parseInt("0", 16), Integer.parseInt("5dc", 16)))
                    .concat(Dollar.$(Integer.parseInt("884", 16), Integer.parseInt("89a", 16)))
                    .concat(Dollar.$(Integer.parseInt("884", 16), Integer.parseInt("89a", 16)))
                    .concat(Dollar.$(Integer.parseInt("b00", 16), Integer.parseInt("b07", 16)))
                    .concat(Dollar.$(Integer.parseInt("bad", 16), Integer.parseInt("baf", 16)))
                    .concat(Dollar.$(Integer.parseInt("1000", 16), Integer.parseInt("10ff", 16)))
                    .concat(Dollar.$(Integer.parseInt("2000", 16), Integer.parseInt("207f", 16)))
                    .concat(Dollar.$(Integer.parseInt("22e0", 16), Integer.parseInt("22f2", 16)))
                    .concat(Dollar.$(Integer.parseInt("86dd", 16), Integer.parseInt("8fff", 16)))
                    .concat(Dollar.$(Integer.parseInt("9000", 16), Integer.parseInt("9003", 16)))
                    .concat(Dollar.$(Integer.parseInt("9040", 16), Integer.parseInt("905f", 16)))
                    .concat(Dollar.$(Integer.parseInt("c020", 16), Integer.parseInt("c02f", 16)))
                    .concat(Dollar.$(Integer.parseInt("c220", 16), Integer.parseInt("c22f", 16)))
                    .concat(Dollar.$(Integer.parseInt("fea0", 16), Integer.parseInt("feaf", 16)))
                    .concat(Dollar.$(Integer.parseInt("ff00", 16), Integer.parseInt("ff0f", 16)))
                    .concat(Integer.parseInt("81c", 16)).concat(Integer.parseInt("844", 16))
                    .concat(Integer.parseInt("900", 16)).concat(Integer.parseInt("a00", 16))
                    .concat(Integer.parseInt("a01", 16)).concat(Integer.parseInt("22df", 16))
                    .concat(Integer.parseInt("9999", 16)).concat(Integer.parseInt("9c40", 16))
                    .concat(Integer.parseInt("a580", 16)).concat(Integer.parseInt("fc0f", 16))
                    .concat(Integer.parseInt("ffff", 16)).sort().toList();

    public Worker(int nodeId) {

        receive(ReceiveBuilder.
                match(WorkerPoolProtocol.Work.class, work -> {
                    ByteIterator it = work.data.iterator();
                    ByteIterator itCopy;
                    int ts_usec = 0;
                    int ts_sec = 0;
                    int bytesSkip = 0;

                    while (it.hasNext()) {
                        itCopy = it.clone();
                        ts_sec = itCopy.getInt(ByteOrder.LITTLE_ENDIAN);
                        ts_usec = itCopy.getInt(ByteOrder.LITTLE_ENDIAN);
                        int incl_len = itCopy.getInt(ByteOrder.nativeOrder());
                        int orig_len = itCopy.getInt(ByteOrder.nativeOrder());
                        itCopy.getBytes(12);
                        int ether_type = itCopy.getShort(ByteOrder.LITTLE_ENDIAN);
                        if (incl_len == orig_len
                                && incl_len <= 65535 && incl_len >= 41
                                && ts_sec > 964696316
                                && validEthertypes.contains(ether_type))
                            break;
                        else
                            it.next();
                        bytesSkip++;
                    }
                    Date timestamp = new Date(ts_sec * 1000L + ts_usec / 1000);
                    int chunkname = work.data.hashCode();
                    //System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(timestamp));
                    //System.out.println(bytesSkip);
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
                    Files.newByteChannel(Paths.get("/tmp/" + nodeId + "/" + chunkname), CREATE, WRITE).write(work.data.toByteBuffer());
                    //Files.write(Paths.get("/tmp/"+ nodeId +"/" + work.data.hashCode()), work.data.toByteBuffer());
                    sender().tell(WorkerPoolProtocol.database_msg("bigFlows.pcap", timestamp, chunkname, bytesSkip), self());
                    //sender().tell(WorkerPoolProtocol.reply(work.data), self());
                }).match(String.class, filename -> {
            System.out.println(filename);
        })
                .build());
    }
}


class ClientActor extends UntypedActor {
    @Override
    public void onReceive(Object message) throws Exception {
       // System.out.println(sender());
    }
}

class DatabaseActor extends UntypedActor  {

    //get list of chunks for given filename and nodeId


    @Override
    public void onReceive(Object message) throws Exception {
       if (message instanceof WorkerPoolProtocol.Database_msg) {
           System.out.println(WorkerPoolProtocol.getChunkList("bigFlows.pcap", 1));
           System.out.println(WorkerPoolProtocol.getNextChunk("bigFlows.pcap", 256895667));
       }
    }
}

public class Application {
    public static List<WorkerPoolProtocol.ChunkPath> getChunkList(String filename) throws Exception{
        DBCollection table = new MongoClient( "localhost" , 27017 ).getDB("test").getCollection("test");
        BasicDBObject whereQuery = new BasicDBObject();
        whereQuery.put("filename", filename);
//        whereQuery.put("actorRef", nodeId);
        DBCursor result = table.find(whereQuery);
        List<WorkerPoolProtocol.ChunkPath> chunks = new ArrayList<>();
        while (result.hasNext()){
            DBObject res = result.next();
            chunks.add(WorkerPoolProtocol.chunkPath(res.get("chunkname").toString(), (int) res.get("actorRef") ));
        }
        return chunks;
    }

    public static List<String> showFiles() throws Exception{
        DBCollection table = new MongoClient( "localhost" , 27017 ).getDB("test").getCollection("test");
        List<String> result = table.distinct("filename");
        return result;
    }

  public static void main(String[] args) throws Exception {
    final ActorSystem system = ActorSystem.create("Sys");
    final ActorMaterializer materializer = ActorMaterializer.create(system);
    final Integer chunkSize = 1024 * 1024 * 20; //bytes
    final String inPath = "/home/laboshinl/Downloads/bigFlows.pcap";
    final File inputFile = new File(inPath);

//    ActorRef replyTo = system.actorOf(Props.create(DatabaseActor.class));

//    FileIO.fromFile(inputFile, chunkSize)
//            .map(i -> WorkerPoolProtocol.msg(i, ActorRef.noSender()))
//            .runWith(Sink.<WorkerPoolProtocol.Msg>actorSubscriber(WorkerPool.props()), materializer);
    System.out.println(showFiles());
    Source.from(getChunkList("bigFlows.pcap")).runWith(Sink.<WorkerPoolProtocol.ChunkPath>actorSubscriber(WorkerPool.props()), materializer);
  }

}
