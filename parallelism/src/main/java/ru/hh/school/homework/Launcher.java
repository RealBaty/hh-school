package ru.hh.school.homework;

import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.reverseOrder;
import static java.util.Map.Entry.comparingByValue;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;

public class Launcher {

  private static final ExecutorService mainExecutor = Executors.newFixedThreadPool(6);
  private static final ExecutorService requestExecutor = Executors.newFixedThreadPool(12);


  public static void main(String[] args) throws IOException, InterruptedException {
    // Написать код, который, как можно более параллельно:
    // - по заданному пути найдет все "*.java" файлы
    // - для каждого файла вычислит 10 самых популярных слов (см. #naiveCount())
    // - соберет top 10 для каждой папки в которой есть хотя-бы один java файл
    // - для каждого слова сходит в гугл и вернет количество результатов по нему (см. #naiveSearch())
    // - распечатает в консоль результаты в виде:
    // <папка1> - <слово #1> - <кол-во результатов в гугле>
    // <папка1> - <слово #2> - <кол-во результатов в гугле>
    // ...
    // <папка1> - <слово #10> - <кол-во результатов в гугле>
    // <папка2> - <слово #1> - <кол-во результатов в гугле>
    // <папка2> - <слово #2> - <кол-во результатов в гугле>
    // ...
    // <папка2> - <слово #10> - <кол-во результатов в гугле>
    // ...
    //
    // Порядок результатов в консоли не обязательный.
    // При желании naiveSearch и naiveCount можно оптимизировать.

    // test our naive methods:
    Path path = Path.of("C:\\");
    getStatistic(path);
    Thread.sleep(1000);
    requestExecutor.shutdown();
    //testCount();
    //testSearch();
  }

  private static void testCount() {
    Path path = Path.of("C:\\Users\\Dima\\HhSchool\\hh-school\\parallelism\\src\\main\\java\\ru\\hh\\school\\homework\\Launcher.java");
    System.out.println(naiveCount(path));
  }

  private static Map<String, Long> naiveCount(Path path) {
    try (var lines = Files.lines(path)){
      return lines
        .flatMap(line -> Stream.of(line.split("[^a-zA-Z0-9]")))
        .filter(word -> word.length() > 3)
        .collect(groupingBy(identity(), counting()))
        .entrySet()
        .stream()
        .sorted(comparingByValue(reverseOrder()))
        .limit(10)
        .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void outputStatistic(Map<String, Long> statistic, Path path){
    for(var word: statistic.keySet()){
      /*try {
        System.out.println("<" + path.toString() + "> - <" + word + "> - <" + naiveSearch(word) + ">");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }*/
      Blackhole.consumeCPU(100);
    }
  }

  private static Map<String, Long> mergeStatistics(Map<String, Long> statistic1, Map<String, Long> statistic2){
    return Stream.of(statistic1, statistic2)
            .flatMap(map -> map.entrySet().stream())
            .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    Long::sum))
            .entrySet()
            .stream()
            .sorted(comparingByValue(reverseOrder()))
            .limit(10)
            .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    Long::sum));
  }

  private static CompletableFuture<List<Path>> getChildrenPaths(Path path){
    CompletableFuture<List<Path>> childrenPaths = new CompletableFuture<>();
    mainExecutor.execute(() -> {
      try(var pathStream = Files.list(path)) {
        childrenPaths.complete(pathStream
                .toList());
      } catch (IOException e) {
        //throw new RuntimeException(e);
        childrenPaths.complete(new ArrayList<>());
      }
    });
    return childrenPaths;
  }

  public static CompletableFuture<Map<String, Long>> getJavaFileStatistic(Path path){
    CompletableFuture<Map<String, Long>> fileStatistic = new CompletableFuture<>();
    if(path.getFileName().toString().matches("[a-zA-Z0-9]+.java")) {
      mainExecutor.execute(() -> fileStatistic.complete(naiveCount(path)));
    } else {
      fileStatistic.complete(new HashMap<>());
    }
    return fileStatistic;
  }

  /*private static void getStatistic(Path path, CompletableFuture<Map<String, Long>> statistic){
    if(!Files.isDirectory(path)){
      statistic.complete(naiveCount(path));
    } else {

    }
  }*/

  private static CompletableFuture<Map<String, Long>> getStatistic(Path path) {
    if(!Files.isDirectory(path)){
      return getJavaFileStatistic(path);
    }
    var res = getChildrenPaths(path)
            .thenApply(pats -> pats.stream()
                    .map(Launcher::getStatistic)
                    .toList())
            .thenCompose(childrenStatistics -> {
              CompletableFuture<Map<String, Long>> statistic = CompletableFuture.completedFuture(new HashMap<>());
              for(var childStatistic: childrenStatistics){
                statistic = statistic.thenCombineAsync(childStatistic, Launcher::mergeStatistics);
              }
              return statistic;
            });
    res.thenAcceptAsync(statistic -> Launcher.outputStatistic(statistic, path), requestExecutor);
    return res;
  }

  private static void testSearch() throws IOException {
    System.out.println(naiveSearch("public"));
  }

  /*private static long naiveSearch(String query) throws IOException {
    Document document = Jsoup //
      .connect("https://www.google.com/search?q=" + query) //
      .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.116 Safari/537.36") //
      .get();

    Element divResultStats = document.select("div#slim_appbar").first();
    String text = divResultStats.text();
    String resultsPart = text.substring(0, text.indexOf('('));
    return Long.parseLong(resultsPart.replaceAll("[^0-9]", ""));
  }*/

  private static long naiveSearch(String query) throws IOException {
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return 228;
  }

}
