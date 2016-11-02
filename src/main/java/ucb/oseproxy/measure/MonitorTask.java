package ucb.oseproxy.measure;

import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Timer;
import java.util.TimerTask;
/* This class represents a timer triggered task that runs every interval
 * that counts the number of adds called in the interval. 
 * Simple case of usage is to count number of queries run per second, for the past second or so.
 * 
 */
public class MonitorTask extends TimerTask {
  private int last, current; // concurrent access to current? 
  private Timer tim;
  private int interval, intcount;
  private PrintStream os; 
  public MonitorTask(int interval, PrintStream os) {
    tim = new Timer();
    current = 0;
    last = 0;
    intcount = 0;
    this.interval = interval;
    tim.schedule(this, 0, interval *1000);
    this.os = os;
  }
  
  public MonitorTask(int interval, String logfilename) throws FileNotFoundException {
    this(interval, new PrintStream(logfilename));
  }
  public MonitorTask(int interval) {
    this(interval, System.out);
  }
  
  public void add() {
    current++;
  }

  @Override
  public void run() {
    int rate = (current-last) / interval;
    last = current;
    os.println(intcount + ", " +  rate);
    intcount ++;
  }

}
