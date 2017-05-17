import org.junit.Before;
import org.junit.Test;
import org.junit.runner.Result;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Test_FoodReview {

    FoodReview foodReview;

    @Before
    public void init() {
        foodReview = new FoodReview();
    }

    @Test
    public void test() {
        foodReview.review(getClass().getClassLoader().getResource("example.csv").getPath().toString());

        Map<String, Long> resultMap = foodReview.getUserIdMap();
        assertEquals("Incorrect active users", 3, resultMap.get("U003").longValue());
        assertEquals("Incorrect active users", 1, resultMap.get("U001").longValue());
        assertEquals("Incorrect active users", 2, resultMap.get("U002").longValue());

        resultMap = foodReview.getFoodIdMap();
        assertEquals("Incorrect count of food", 1, resultMap.get("P001").longValue());
        assertEquals("Incorrect count of food", 2, resultMap.get("P002").longValue());
        assertEquals("Incorrect count of food", 3, resultMap.get("P003").longValue());

        resultMap = foodReview.getWordMap();
        assertEquals("Incorrect count of food", 1, resultMap.get("one_3").longValue());
        assertEquals("Incorrect count of food", 3, resultMap.get("three_2").longValue());
        assertEquals("Incorrect count of food", 2, resultMap.get("two_1").longValue());
    }


}
