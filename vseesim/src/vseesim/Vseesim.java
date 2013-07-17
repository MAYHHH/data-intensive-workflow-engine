
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
    static double time;
    static double latency;
    static double bandWidth;
    
    public static void init()
    {
        //TODO: init workflow
        //TODO: init machines
        //TODO: schedule workflow
        //TODO: workflow partitioning
        scheduleNextTaskFinishEvent();
    }
    
    public static void run()
    {
        Event e = eq.poll();
        time = e.getTime();
        switch(e.getType())
        {
            case Event.TYPE_FILE_SENT:
                scheduleNextTaskFinishEvent();
                break;
            case Event.TYPE_TASK_FIN:
                Event newEvent = new Event(Event.TYPE_FILE_SENT);
                Partition p = (Partition)e.getProperty("Partition");
                newEvent.setTime(time+(1*latency+(p.getOutputsize()/bandWidth)));
                eq.add(newEvent);
                break;
            
        }
    }
    public static Partition getReadyPartition()
    {
        return null;
    }
    public static void scheduleNextTaskFinishEvent()
    {
        Partition p;
        while((p = getReadyPartition()) != null)
        {
            Event e = new Event(Event.TYPE_TASK_FIN);
            Machine m = hm.get(p);
            e.setProperty("Partition", p);
            e.setProperty("Machine", m);
            e.setTime(time+p.calExecutionTime(m));
            eq.add(e);
        }
    }
    public static void main(String[] args)
    {
        // TODO code application logic here
    }
}
