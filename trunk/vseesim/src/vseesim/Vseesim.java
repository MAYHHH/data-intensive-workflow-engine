
package vseesim;

import java.util.HashMap;
import java.util.PriorityQueue;

/**
 *
 * @author Dew
 */
public class Vseesim
{
    static PriorityQueue<Event> eq = new PriorityQueue<>();
    static HashMap<Partition,Machine> hm= new HashMap<>();
    static int time;
    
    public static void init()
    {
        Event e = new Event(Event.TYPE_TASK_FIN);
        Partition p = getReadyPartition();
        Machine m = hm.get(p);
        e.setProperty("Partition", p);
        e.setProperty("Machine", m);
        e.setTime(time+p.calExecutionTime(m));
        eq.add(e);
        
        
    }
    
    public static void run()
    {
        
    }
    public static Partition getReadyPartition()
    {
        return null;
    }
    public static void main(String[] args)
    {
        // TODO code application logic here
    }
}
