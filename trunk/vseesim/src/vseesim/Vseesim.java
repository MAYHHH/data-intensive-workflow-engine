
package vseesim;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Random;
import workflowengine.resource.Worker;
import workflowengine.schedule.Schedule;
import workflowengine.workflow.Task;
import workflowengine.workflow.Workflow;

/**
 *
 * @author Dew
 */
public class Vseesim
{
    static PriorityQueue<Event> eq = new PriorityQueue<>();
    static HashMap<Partition,Machine> partitionMapping= new HashMap<>();
    static double simTime;
    static double LATENCY_MEAN = 1;
    static double LATENCY_SD = 0.3;
    static double BANDWIDTH_MEAN = 10;
    static double BANDWIDTH_SD = 3;
    static Random RANDOM = new Random();
    
    
    public static void init()
    {
        //TODO: init workflow
        //TODO: init machines
        //TODO: schedule workflow
        //TODO: workflow partitioning
        scheduleNextTaskFinishEvent();
        run();
    }
    
    public static void run()
    {
        Event e ;
        while((e = eq.poll()) != null)
        {
            simTime = e.getTime();
            switch(e.getType())
            {
                case Event.TYPE_FILE_SENT:
                    System.out.println(e);
                    Machine to = (Machine)e.getProperty("to_machine");
                    to.addSnapshot((Snapshot)e.getProperty("snapshot"));
                    scheduleNextTaskFinishEvent();
                    break;
                case Event.TYPE_TASK_FIN:
                    System.out.println(e);
                    Partition p = (Partition)e.getProperty("Partition");
                    Machine m = (Machine)e.getProperty("Machine");
                    m.addSnapshot(p.getSnapshot());
                    p.setFinished(true);
                    partitionMapping.remove(p);
                    scheduleFileSentFromPartition(p, m);
                    scheduleNextTaskFinishEvent();
                    break;
            }
        }
        System.out.println("No more event. Simulation is stopped."+simTime);
    }
    
    public static void scheduleFileSentFromPartition(Partition par, Machine curM)
    {
        double finishTime = simTime;
        for(Partition p : par.children)
        {
            Machine m = partitionMapping.get(p);
            if(curM.equals(m))
            {
                continue;
            }
            Event e = new Event(Event.TYPE_FILE_SENT);
            e.setProperty("from_machine", curM);
            e.setProperty("to_machine", m);
            e.setProperty("snapshot", par.getSnapshot());
            finishTime += m.getTransferTime(par.getSnapshot());
            e.setTime(finishTime);
            eq.add(e);
        }
    }
    
    public static Partition getReadyPartition()
    {
        for(Partition p : partitionMapping.keySet())
        {
            Machine m = partitionMapping.get(p);
            if(!p.isExecuting() && p.hasParentSnapshotsOn(m))
            {
                return p;
            }
        }
        return null;
    }
    public static void scheduleNextTaskFinishEvent()
    {
        Partition p;
        while((p = getReadyPartition()) != null)
        {
            p.setExecuting(true);
            Event e = new Event(Event.TYPE_TASK_FIN);
            Machine m = partitionMapping.get(p);
            e.setProperty("Partition", p);
            e.setProperty("Machine", m);
            e.setTime(simTime+m.getExecTime(p));
            eq.add(e);
        }
    }
    public static void main(String[] args)
    {
        Partition p1 = new Partition();
        Partition p2 = new Partition();
        Partition p3 = new Partition();
        Partition p4 = new Partition();
        Partition p5 = new Partition();
        p1.setWorkload(RANDOM.nextInt(2000)+500);
        p2.setWorkload(RANDOM.nextInt(2000)+500);
        p3.setWorkload(RANDOM.nextInt(2000)+500);
        p4.setWorkload(RANDOM.nextInt(2000)+500);
        p5.setWorkload(RANDOM.nextInt(2000)+500);
        p1.addChild(p2);
        p1.addChild(p3);
        p2.addChild(p4);
        p4.addChild(p5);
        p3.addChild(p5);
        p1.setSnapshot(new Snapshot(100));
        p2.setSnapshot(new Snapshot(200));
        p3.setSnapshot(new Snapshot(300));
        p4.setSnapshot(new Snapshot(400));
        p5.setSnapshot(new Snapshot(500));
        Machine m1 = new Machine();
        Machine m2 = new Machine();
        partitionMapping.put(p1, m1);
        partitionMapping.put(p2, m2);
        partitionMapping.put(p3, m1);
        partitionMapping.put(p4, m2);
        partitionMapping.put(p5, m1);
        init();
    }
    
    public HashMap<Task, LinkedList<Task>> Partition(Workflow w, Schedule s)
    {
         HashMap<Task, LinkedList<Task>> groups = new HashMap<>();
         LinkedList<Task> q = new LinkedList<>();
         q.add(w.getStartTask());
         while (!q.isEmpty())
         {
             Task node = q.poll();
             LinkedList<Task> group = null;
             Worker wk = s.getWorkerForTask(node);
             for (Task Parent : w.getParentTasks(node))
             {
                 Worker wkp = s.getWorkerForTask(Parent);
                 if (wk.equals(wkp))
                 {
                     group = groups.get(Parent);
                 }
                 
             }
             if (group == null)
             {
                 group = new LinkedList<>();
                 
             }
             group.add(node);
             groups.put(node, group);
             q.addAll(w.getChildTasks(node));
            
         }
             return groups;
    }
    
}
