package ucb.oseproxy.measure;

import java.util.Timer;
import java.util.TimerTask;

public class MonitorTask extends TimerTask {
  private int last, current;
  private Timer tim;
  private int interval, intcount;
  public MonitorTask(int interval) {
    tim = new Timer();
    current = 0;
    last = 0;
    intcount = 0;
    this.interval = interval;
    tim.schedule(this, 0, interval *1000 );
    
  }
  
  public void add() {
    current++;
  }

  @Override
  public void run() {
    int rate = (current-last) / interval;
    last = current;
    System.out.println(intcount + ", " +  rate);
    intcount ++;
  }

}
