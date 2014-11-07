package sample.stream;

import akka.actor.ActorSystem;
import akka.dispatch.OnComplete;
import akka.stream.FlowMaterializer;
import akka.stream.javadsl.Source;
import scala.runtime.BoxedUnit;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static akka.japi.Option.option;

public class GroupLogFile {
  public static void main(String[] args) throws IOException {
    final ActorSystem system = ActorSystem.create("Sys");
    final FlowMaterializer materializer = FlowMaterializer.create(system);

    final Pattern loglevelPattern = Pattern.compile(".*\\[(DEBUG|INFO|WARN|ERROR)\\].*");

    // read lines from a log file
    final String inPath = "src/main/resources/logfile.txt";
    final BufferedReader fileReader = new BufferedReader(new FileReader(inPath));

    Source
      .from(() -> option(fileReader.readLine())).
      // group them by log level
        groupBy(line -> {
        final Matcher matcher = loglevelPattern.matcher(line);
        if (matcher.find()) return matcher.group(1);
        else return "OTHER";
      }).
      // write lines of each group to a separate file
        foreach(levelProducerPair -> {
        final String outPath = "target/log-" + levelProducerPair.first() + ".txt";
        final PrintWriter output = new PrintWriter(new FileOutputStream(outPath), true);

        levelProducerPair.second().
          foreach(output::println, materializer).
          // close resource when the group stream is completed
            onComplete(new OnComplete<BoxedUnit>() {
            @Override public void onComplete(Throwable failure, BoxedUnit success) throws Exception {
              output.close();
            }
          }, system.dispatcher());
      }, materializer).
      onComplete(new OnComplete<BoxedUnit>() {
        @Override public void onComplete(Throwable failure, BoxedUnit success) throws Exception {
          try { fileReader.close(); } catch (IOException ignore) { } finally { system.shutdown(); }
        }
      }, system.dispatcher());
  }
}