import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.*;

public class FoodReview {

    private Set<Long> reviewIdSet = new HashSet<>();
    private ConcurrentMap<String, Long> userIdMap = new ConcurrentHashMap<>();
    private ConcurrentMap<String, Long> foodIdMap = new ConcurrentHashMap<>();
    private ConcurrentMap<String, Long> wordMap = new ConcurrentHashMap<>();

    private ExecutorService exeUserViewer = Executors.newFixedThreadPool(25);
    private ExecutorService exeFoodViewer = Executors.newFixedThreadPool(25);
    private ExecutorService exeWordViewer = Executors.newFixedThreadPool(50);

    public static void main(String[] arg) {

        //String fName = "/home/partiza/Reviews.csv";
        FoodReview foodReviews = new FoodReview();
        String fName = arg[0];
        foodReviews.review(fName);
        foodReviews.printTopUsers(1000);
        foodReviews.printTopFoods(1000);
        foodReviews.printTopWords(1000);

    }

    public ConcurrentMap<String, Long> getUserIdMap() {
        return userIdMap;
    }

    public ConcurrentMap<String, Long> getFoodIdMap() {
        return foodIdMap;
    }

    public ConcurrentMap<String, Long> getWordMap() {
        return wordMap;
    }

    public void review(String fileName) {

        try (BufferedReader br = Files.newBufferedReader(Paths.get(fileName))) {
            br.readLine();
            br.lines().forEach(this::apply);
        } catch (IOException e) {
            e.printStackTrace();
        }

        exeUserViewer.shutdown();
        exeFoodViewer.shutdown();
        exeWordViewer.shutdown();

        try {
            exeUserViewer.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            exeFoodViewer.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            exeWordViewer.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private void apply(String line) {
        String[] data = line.split(",");
        Long reviewId = Long.valueOf(data[0]);

        // check duplicates
        if (!reviewIdSet.contains(reviewId)) {
            reviewIdSet.add(reviewId);

            // users activity count
            exeUserViewer.execute(() -> {
                        userIdMap.compute(data[2], (k, v) -> (v == null) ? 1 : v + 1);
                    }
            );

            // number of comments on food items
            exeFoodViewer.execute(() -> {
                foodIdMap.compute(data[1], (k, v) -> (v == null) ? 1 : v + 1);
            });


            // count words
            exeWordViewer.execute(() -> {
                // google translate API may be used here
                String[] words = data[9].split("\\s+");
                Arrays.stream(words).forEach(w -> {
                    wordMap.compute(w, (k, v) -> (v == null) ? 1 : v + 1);
                });
            });

        }
    }

    public void printTopUsers(int topN) {
        System.out.println("Most active users");
        userIdMap.entrySet().stream().parallel().sorted(Map.Entry.<String, Long>comparingByValue().reversed()).limit(topN).forEachOrdered(e -> {
            System.out.println("userId = " + e.getKey() + "; cnt = " + e.getValue());
        });
    }

    public void printTopFoods(int topN) {
        System.out.println("Most commented food items");
        foodIdMap.entrySet().stream().parallel().sorted(Map.Entry.<String, Long>comparingByValue().reversed()).limit(topN).forEachOrdered(e -> {
            System.out.println("foodId = " + e.getKey() + "; cnt = " + e.getValue());
        });
    }

    public void printTopWords(int topN) {
        System.out.println("Most used words in reviews");
        wordMap.entrySet().stream().sorted(Map.Entry.<String, Long>comparingByValue().reversed()).limit(topN).forEach(e -> {
            System.out.println("wordId = " + e.getKey() + "; cnt = " + e.getValue());
        });
    }

}
