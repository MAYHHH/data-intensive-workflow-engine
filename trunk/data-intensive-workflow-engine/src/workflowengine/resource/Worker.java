/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.resource;

import java.util.HashMap;
import workflowengine.utils.DBRecord;
import workflowengine.utils.Utils;
import workflowengine.workflow.Task;

/**
 *
 * @author Orachun
 */
public class Worker
{
//    private static ArrayList<Worker> workers = new ArrayList<>();
    private static int count = 0;
    private int id;
    private String name;
    private double cpu; //Operations per time unit: represent server's performance
    private double unitCost; //cost per time unit
    private double freeStorage; //Storage in MB
    private double freeMemory; //Storage in MB
    private int dbid;
    private int currentTaskDbid = -1;
//    private double usedStorage = 0;
//    private HashMap<String, WorkflowFile> fileSystem = new HashMap<>();
    private HashMap<String, Object> objProbs = new HashMap<>();

    private Worker(String name, double cpu, double unitCost, double freeStorage, double freeMemory)
    {
        this.name = name;
        this.cpu = cpu;
        this.unitCost = unitCost;
        this.freeStorage = freeStorage;
        this.freeMemory = freeMemory;
    }

    private Worker(String name, double cpu, double freeStorage, double unitCost)
    {
        this(name, cpu, freeStorage, unitCost, -1);
    }
    private Worker(String name, double cpu, double freeStorage, double unitCost, int dbid)
    {
        this.name = name;
        this.cpu = cpu;
        this.freeStorage = freeStorage;
        id = count++;
        this.unitCost = unitCost;
        this.dbid = dbid;
    }
    
    public static Worker getWorker(String name, double cpu, double storage, double unitCost)
    {
        Worker w = getWorkerFromDB(name);
        if(w == null)
        {
            w = new Worker(name, cpu, storage, unitCost);
            w.insert();
        }
        return w;
        
    }
    
    public int getDbid()
    {
        return dbid;
    }
    
    public static Worker getWorkerFromDB(String name)
    {
        try
        {
            DBRecord r = DBRecord.select("worker",
                    "SELECT * "
                    + "FROM worker "
                    + "WHERE hostname='" + name + "'").get(0);
            Worker w = new Worker(r.get("name"), r.getDouble("cpu"), r.getDouble("free_space"), r.getDouble("unit_cost"), r.getInt("wkid"));
            return w;
        }
        catch (IndexOutOfBoundsException ex)
        {
            return null;
        }
    }
    public static Worker getWorkerFromDB(int dbid)
    {
        try
        {
            DBRecord r = DBRecord.select("worker",
                    "SELECT * "
                    + "FROM worker "
                    + "WHERE wkid='" + dbid + "'").get(0);
            Worker w = new Worker(r.get("name"), r.getDouble("cpu"), r.getDouble("free_space"), r.getDouble("unit_cost"), r.getInt("wkid"));
            return w;
        }
        catch (IndexOutOfBoundsException ex)
        {
            return null;
        }
    }
    
    public void insert()
    {
        new DBRecord("worker", 
                "hostname", name, 
                "cpu", cpu, 
                "updated", Utils.time(),
                "free_space", freeStorage,
                "free_memory", freeMemory
        ).insert();
    }
    
    public static void updateWorkerStatus(String hostname, int currentTid, double freeMem, double freeStorage, double cpu)
    {
        Worker w = getWorkerFromDB(hostname);
        if(w == null)
        {
            w = new Worker(hostname, cpu, freeStorage, 0, freeMem);
            w.currentTaskDbid = currentTid;
            w.insert();
        }
        else
        {
            new DBRecord("worker", 
                    "current_tid", currentTid, 
                    "updated", Utils.time(),
                    "free_memory", freeMem,
                    "free_space", freeStorage,
                    "cpu", cpu
            ).update(new DBRecord("worker",
                    "hostname", hostname
            ));
        }
        
    }
    
    
    public boolean equalTo(Object o)
    {
        if(o instanceof Worker)
        {
            return this.name.equals(((Worker)o).name);
        }
        else
        {
            return false;
        }
    }

//    public static Worker get(int id)
//    {
//        return workers.get(id);
//    }
    public int getId()
    {
        return id;
    }

    public String getName()
    {
        return name;
    }

    public double getOps()
    {
        return cpu;
    }

    public double getExecTime(Task t)
    {
        return t.getOperations() / cpu;
    }
    
    public double getUnitCost()
    {
        return unitCost;
    }
    
    public double getExecCost(Task t)
    {
        return unitCost*getExecTime(t);
    }

//    public boolean addFile(WorkflowFile f)
//    {
//        if (storage - usedStorage > f.getSize())
//        {
//            fileSystem.put(f.getName(), f);
//            usedStorage += f.getSize();
//            return true;
//        }
//        else
//        {
//            return false;
//        }
//    }
//    public WorkflowFile delFile(String filename)
//    {
//        if(fileSystem.containsKey(filename))
//        {
//            WorkflowFile f = fileSystem.get(filename);
//            fileSystem.remove(filename);
//            usedStorage -= f.getSize();
//            return f;
//        }
//        else
//        {
//            return null;
//        }
//    }
//    public WorkflowFile getFile(String filename)
//    {
//        return fileSystem.get(filename);
//    }
//    
    public String getEdgeName(Worker s)
    {
        if(this.id<s.id)
        {
            return "("+this.id+","+s.id+")";
        }
        else
        {
            return "("+s.id+","+this.id+")";
        }
    }
    public void setProp(String name, Object o)
    {
        objProbs.put(name, o);
    }
    public Object getProp(String name)
    {
        return objProbs.get(name);
    }
    public Double getDoubleProp(String name)
    {
        return (Double)objProbs.get(name);
    }
    public String toString()
    {
        return name+"("+cpu+")";
    }
}
