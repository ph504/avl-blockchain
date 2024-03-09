package approach4;

import java.time.Duration;
import java.time.Instant;

public class MyTimer {
    private Instant timer;
    private long aggregatedTime;

    public MyTimer() {
        init();
    }

    public void start() throws Exception {
        if (this.timer != null) {
            throw new Exception("timer already in progress");
        }

        this.aggregatedTime = 0;
        this.timer = Instant.now();
    }

    public void pause() throws Exception {
        if (this.timer == null) {
            throw new Exception("timer is not in progress");
        }

        long elapsed = Duration.between(this.timer, Instant.now()).toNanos();
        this.aggregatedTime += elapsed;
        this.timer = null;
    }

    public void resume() throws Exception {
        if (this.timer != null) {
            throw new Exception("timer already in progress");
        }

        this.timer = Instant.now();
    }

    public double getElapsedNanoSeconds() throws Exception {
        if (this.timer != null) {
            throw new Exception("timer still in progress");
        }

        return this.aggregatedTime;
    }

    public double getElapsedMilliSeconds() throws Exception {
        return getElapsedNanoSeconds()/1_000_000;
    }

    public double getElapsedMicroSeconds() throws Exception {
        return getElapsedNanoSeconds()/1_000;
    }

    public double getElapsedSeconds() throws Exception {
        return getElapsedNanoSeconds()/1_000_000_000;
    }

    public void init() {
        this.timer = null;
        this.aggregatedTime = Long.MIN_VALUE;
    }




}
