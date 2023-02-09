package ru.hh.school.homework;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import static org.slf4j.LoggerFactory.getLogger;

public class Launcher {

  private static final ExecutorService mainExecutor = Executors.newFixedThreadPool(12);

  // Т. к. узким горлышком являются долгие запросы, то если здесь использовать Executors.newVirtualThreadPerTaskExecutor(), можно значительно
  // ускорить время выполнения.
  // Однако гугл за такое не поблагодарит. Также можно увеличить количество потоков в тред пуле. Это значительно увеличит производительность,
  // и приведет к более быстрому бану.
  private static final ExecutorService requestExecutor = Executors.newFixedThreadPool(20);

  // При большом количестве потоков в requestExecutor, или при использовании Executors.newVirtualThreadPerTaskExecutor()
  // выполнение вывода в консоль в отдельном потоке дает значительное увеличение производительности,
  // так как вывод в консоль - synchronized операция
  private static final ExecutorService outputExecutor = Executors.newSingleThreadExecutor();

  private static final Logger LOGGER = getLogger(Launcher.class);


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
    // "C:\\Users\\Dima\\HhSchool\\hh-school\\parallelism\\src\\main\\java\\ru\\hh\\school\\homework\\Launcher.java"
    // test our naive methods:
    Path path = Path.of("C:\\Users\\Dima\\HhSchool\\hh-school\\parallelism\\src\\main\\java\\ru\\hh\\school\\parallelism\\");
    getAndOutputStatistic(path);
  }

  private static Map<String, Long> count(Path path) {
    try (var lines = Files.lines(path)){
      return lines
        .flatMap(line -> Stream.of(line.split("[^a-zA-Z0-9]")))
        .filter(word -> word.length() > 3)
        .collect(groupingBy(identity(), counting()));
    }
    catch (IOException e) {
      LOGGER.error("The file <" + path + "> cannot be read");
      return new HashMap<>();
    }
  }

  private static void outputStatistic(Map<String, Long> statistic, Path path){
    List<String> mostPopularWords = statistic.entrySet().stream()
            .sorted(comparingByValue(reverseOrder()))
            .limit(10)
            .map(Map.Entry::getKey)
            .toList();
    for(var word: mostPopularWords){
      CompletableFuture.supplyAsync(() -> search(word), requestExecutor)
              .thenAcceptAsync(resultCount -> System.out.println("<" + path.toString() + "> - <" + word + "> - <" + resultCount + ">"),
                       outputExecutor);
    }
  }

  private static Map<String, Long> mergeStatistics(Map<String, Long> statistic1, Map<String, Long> statistic2){
    return Stream.of(statistic1, statistic2)
            .flatMap(map -> map.entrySet().stream())
            .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    Long::sum));
  }

  public static CompletableFuture<Map<String, Long>> getJavaFileStatistic(Path path){
    CompletableFuture<Map<String, Long>> fileStatistic = new CompletableFuture<>();
    if(path.getFileName().toString().matches(".+.java")) {
      mainExecutor.execute(() -> fileStatistic.complete(count(path)));
    } else {
      fileStatistic.complete(new HashMap<>());
    }
    return fileStatistic;
  }

  private static CompletableFuture<Map<String, Long>> getAndOutputStatistic(Path path) {
    if(!Files.isDirectory(path)){
      return getJavaFileStatistic(path);
    }
    try(var childrenPathsStream = Files.list(path)){
      List<CompletableFuture<Map<String, Long>>> childrenStatisticsPromise = childrenPathsStream
              .map(CompletableFuture::completedFuture)
              .map(pathPromise -> pathPromise.thenComposeAsync(Launcher::getAndOutputStatistic, mainExecutor))
              .toList();
      CompletableFuture<Map<String, Long>> promise = CompletableFuture.completedFuture(new HashMap<>());
      for(var childStatisticPromise: childrenStatisticsPromise){
        promise = promise.thenCombineAsync(childStatisticPromise, Launcher::mergeStatistics, mainExecutor);
      }
      promise.thenAcceptAsync(statistic -> Launcher.outputStatistic(statistic, path), mainExecutor);
      return promise;
    } catch (IOException e){
      LOGGER.error("The folder <" + path + "> cannot be read");
      return CompletableFuture.completedFuture(new HashMap<>());
    }
  }

  private static long search(String query) {
    Document document = null;
    try {
      document = Jsoup //
        .connect("https://www.google.com/search?q=" + query) //
        .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.116 Safari/537.36") //
        .get();
    } catch (IOException e) {
      LOGGER.error("Unable connect to https://www.google.com/search?q=" + query);
      return -1;
    }

    Element divResultStats = document.select("div#result-stats").first();
    if(divResultStats == null){
      LOGGER.error("Invalid page received on https://www.google.com/search?q=" + query);
      return -1;
    }
    String text = divResultStats.text();
    String resultsPart = text.substring(0, text.indexOf('('));
    return Long.parseLong(resultsPart.replaceAll("[^0-9]", ""));
  }

//  private static long search(String query) {
//    try {
//      Thread.sleep(1000);
//    } catch (InterruptedException e) {
//      throw new RuntimeException(e);
//    }
//    return new Random().nextInt(1000000000);
//  }

//  private static Map<String, Long> getStatisticInOneThread(Path path) {
//    if(!Files.isDirectory(path)){
//      if(path.getFileName().toString().matches("[a-zA-Z0-9]+.java")) {
//        return count(path);
//      } else {
//        return new HashMap<>();
//      }
//    }
//    try(var childrenPathsStream = Files.list(path)) {
//      Map<String, Long> statistic = new HashMap<>();
//      for(var childPath: childrenPathsStream.toList()){
//        statistic = mergeStatistics(statistic, getStatisticClassic(childPath));
//      }
//      return statistic;
//    } catch (IOException e){
//      return new HashMap<>();
//    }
//  }

}
