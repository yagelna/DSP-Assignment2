package bgu.ds;

public class Main {
    public static void main(String[]args) {
        WordCount wordCount = new WordCount();
        try {
            wordCount.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
