package ru.laboshinl.tractor;

import akka.actor.ActorRef;
import akka.util.ByteString;
import com.mongodb.*;

import java.util.*;

public class WorkerPoolProtocol {

    public static List<String> getChunkList(String filename, int nodeId) throws Exception{
        DBCollection table = new MongoClient( "localhost" , 27017 ).getDB("test").getCollection("test");
        BasicDBObject whereQuery = new BasicDBObject();
        whereQuery.put("filename", filename);
        whereQuery.put("actorRef", nodeId);
        DBCursor result = table.find(whereQuery);
        List<String> chunks = new ArrayList<>();
        while (result.hasNext()){
            chunks.add(result.next().get("chunkname").toString());
        }
        return chunks;
    }

    public static Map<String, String> getNextChunk(String filename, Integer chunkName) throws Exception{
        Map<String, String> nextChunk = new HashMap<>();
        DBCollection table = new MongoClient( "localhost" , 27017 ).getDB("test").getCollection("test");
        BasicDBObject whereQuery = new BasicDBObject();
        whereQuery.put("filename", filename);
        whereQuery.put("chunkname", chunkName);
        DBCursor result = table.find(whereQuery);
        //DBObject timestamp = result.next().get("timestamp");
        //System.out.println(timestamp);
        whereQuery = new BasicDBObject();
        whereQuery.put("filename", filename);
        whereQuery.put("timestamp", result.next().get("timestamp"));
        DBObject obj = table.find(whereQuery).sort(new BasicDBObject("timestamp", 1)).limit(1).next();
        nextChunk.put(obj.get("chunkname").toString(), obj.get("actorRef").toString());
        return nextChunk;
    }

    public static void insertRecord(Database_msg message) throws Exception{
        DBCollection table = new MongoClient( "localhost" , 27017 ).getDB("test").getCollection("test");
        BasicDBObject document = new BasicDBObject();
        document.put("filename", ((Database_msg) message).filename);
        document.put("timestamp", ((Database_msg) message).timestamp);
        document.put("actorRef", 1);
        document.put("bytesSkip", ((Database_msg) message).offset);
        document.put("chunkname", ((Database_msg) message).chunkname);
        table.insert(document);
    }

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

    public static class ChunkPath{
        public final String chunkname;
        public final Integer nodeid;
        public ChunkPath(String chunkname, Integer nodeid){
            this.chunkname = chunkname;
            this.nodeid=nodeid;
        }
    }

    public static ChunkPath chunkPath(String chunkname, Integer nodeid){return new ChunkPath(chunkname,nodeid);}

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

    public static class Database_msg{
        public final int offset;
        public final Date timestamp;
        public final int chunkname;
        public final String filename;
        public Database_msg(String filename, Date timestamp, int chunkname, int offset  ){
            this.chunkname = chunkname;
            this.filename = filename;
            this.offset = offset;
            this.timestamp = timestamp;
        }
        @Override
        public String toString() {
            return String.format("Name (%s) time(%s)  chunk(%s) offset(%s)", filename, timestamp, chunkname, offset   );
        }
    }
    public static Database_msg database_msg(String filename, Date timestamp, int chunkname, int offset) {
        return new Database_msg(filename, timestamp, chunkname, offset);
    }


//    public static class Done {
//        public final ByteString data;
//        public Done(ByteString data) { this.data = data; }
//
//        @Override
//        public String toString() {
//            return String.format("Done(%s)", data);
//        }
//
//        @Override
//        public boolean equals(Object o) {
//            if (this == o) {
//                return true;
//            }
//            if (o == null || getClass() != o.getClass()) {
//                return false;
//            }
//
//            Done done = (Done) o;
//
//            if (data != done.data) {
//                return false;
//            }
//
//            return true;
//        }
//
//        @Override
//        public int hashCode() {
//            return data.length();
//        }
//    }
//    public static Done done(ByteString data) {
//        return new Done(data);
//    }

}
