package workflowengine.workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import workflowengine.utils.DBException;
import workflowengine.utils.DBRecord;

/**
 *
 * @author Orachun
 */
public class Task
{
    private static int count = 0;
    private int id;
    private String workflowName;
    private int wfdbid;
//    private Workflow wf;
    private String name = "";
    private double operations;//Represent estimated execution time
    private HashMap<String, Object> objProps = new HashMap<>();
    private int dbid = -1;
    private char status = 'W'; // 'W'aiting, 'E'xecuting, 'C'ompleted
    private String cmd;
    private Task(String name, double operations, Workflow wf, String cmd)
    {
        this.name = name;
        this.operations = operations;
        id = ++count;
        this.workflowName = wf.getName();
        this.wfdbid = wf.getDbid();
        this.cmd = cmd;
    }

    private Task(String name, double operations, String workflowName, int wfdbid, String cmd, int dbid)
    {
        this.name = name;
        this.workflowName = workflowName;
        this.wfdbid = wfdbid;
        this.operations = operations;
        this.dbid = dbid;
        this.cmd = cmd;
    }
    
    public static Task getWorkflowTask(String name, double operations, Workflow wf, String cmd)
    {
        Task t = getWorkflowTaskFromDB(name, wf.getName());
        if(t == null)
        {
            t = new Task(name, operations, wf, cmd);
            t.insert();
        }
        return t;
    }
    
    public static Task getWorkflowTaskFromDB(String name, String wfname)
    {
        try
        {
            DBRecord r = DBRecord.select("workflow_task",
                    "SELECT t.name, t.estopr, t.wfid, w.name as wname, t.tid, t.cmd, t.status "
                    + " FROM workflow_task t JOIN workflow w ON t.wfid = w.wfid "
                    + " WHERE t.name='" + wfname+":"+name + "'").get(0);
            Task t = new Task(r.get("name"), r.getDouble("estopr"), r.get("wname"), r.getInt("wfid"), r.get("cmd"), r.getInt("tid"));
            t.status = r.get("status").charAt(0);
            return t;
        }
        catch (IndexOutOfBoundsException ex)
        {
            return null;
        }
    }
    public static Task getWorkflowTaskFromDB(int dbid) throws DBException
    {
        try
        {
            DBRecord r = DBRecord.select("workflow_task",
                    "SELECT t.name, t.estopr, t.wfid, w.name as wname, t.tid, t.cmd, t.status "
                    + " FROM workflow_task t JOIN workflow w ON t.wfid = w.wfid "
                    + " WHERE t.tid='" + dbid + "'").get(0);
            Task t = new Task(r.get("name"), r.getDouble("estopr"), r.get("wname"), r.getInt("wfid"), r.get("cmd"), r.getInt("tid"));
            t.status = r.get("status").charAt(0);
            return t;
        }
        catch (ArrayIndexOutOfBoundsException ex)
        {
            return null;
        }
    }
    
    public int getWfdbid()
    {
        return wfdbid;
    }

    public char getStatus()
    {
        return status;
    }
    
    public void setCmd(String cmd)
    {
        this.cmd = cmd;
        DBRecord.update("UPDATE workflow_task SET cmd='"+cmd+"' WHERE tid='"+dbid+"'");
    }
    public String getCmd()
    {
        return cmd;
    }
    
    public void insert()
    {
        if(dbid == -1)
        {
            DBRecord rec = new DBRecord("workflow_task", 
                    "wfid", wfdbid, 
                    "name", toString(), 
                    "status", status, 
                    "estopr", operations,
                    "cmd", cmd
            );
            dbid = rec.insert();
        }
        else
        {
            throw new IllegalStateException("This task is already inserted.");
        }
    }
//    public void update()
//    {
//        if(dbid != -100)
//        {
//            DBRecord rec = new DBRecord("workflow_task", "wfid", wfdbid, "name", toString(), "status", status, "estopr", operations);
//            dbid = rec.update(new DBRecord("workflow_task", "tid", dbid));
//        }
//        else
//        {
//            throw new IllegalStateException("This task is not inserted yet.");
//        }
//    }
    public static int updateTaskStatus(int tid, long start, long end, int exitValue, char status)
    {
        return DBRecord.update("UPDATE workflow_task "
                + "SET start='"+start+"', finish='"+end+"', exit_value='"+exitValue+"', status='"+status+"' "
                + "WHERE tid='"+tid+"'");
    }
    
    public void addInputFile(WorkflowFile f)throws DBException
    {
        new DBRecord("workflow_task_file", "type", "I", "tid", dbid, "fid", f.getDbid()).insert();
    }
    public void addOutputFile(WorkflowFile f)throws DBException
    {
        new DBRecord("workflow_task_file", "type", "O", "tid", dbid, "fid", f.getDbid()).insert();
    }

    public int getID()
    {
        return id;
    }

    public String getName()
    {
        return toString();
    }

    public String getEdgeName(Task to)
    {
        return "(" + id + "," + to.id + ")";
    }

    public int getDbid()
    {
        return dbid;
    }

    public double getOperations()
    {
        return operations;
    }

//    public void addInputFile(WorkflowFile f)
//    {
//        inputFiles.put(f.getName(), f);
//    }
//
//    public void addOutputFile(WorkflowFile f)
//    {
//        outputFiles.put(f.getName(), f);
//    }
    public List<WorkflowFile> getInputFiles() throws DBException
    {
        List<DBRecord> results = DBRecord.select("workflow_task_file", new DBRecord("workflow_task_file", "type", "I", "wtif", dbid));
        List<WorkflowFile> files = new ArrayList<>(results.size());
        for (DBRecord r : results)
        {
            WorkflowFile f = WorkflowFile.getFile(r.get("name"), Integer.parseInt(r.get("estsize")));
            files.add(f);
        }
        return files;
    }

    public List<WorkflowFile> getOutputFiles() throws DBException
    {
        List<DBRecord> results = DBRecord.select("workflow_task_file", new DBRecord("workflow_task_file", "type", "O", "wtif", dbid));
        List<WorkflowFile> files = new ArrayList<>(results.size());
        for (DBRecord r : results)
        {
            WorkflowFile f = WorkflowFile.getFile(r.get("name"), Integer.parseInt(r.get("estsize")));
            files.add(f);
        }
        return files;
    }

    public void setProp(String name, Object o)
    {
        objProps.put(name, o);
    }

    public Object getProp(String name)
    {
        return objProps.get(name);
    }

    public Double getDoubleProp(String name)
    {
        return (Double) objProps.get(name);
    }

    @Override
    public String toString()
    {
        return workflowName +"(" + wfdbid + ")" + ":" + name;
    }

    @Override
    public boolean equals(Object o)
    {
        return (o instanceof Task && this.toString().equals(o.toString()));
    }

    @Override
    public int hashCode()
    {
        int hash = 3;
        hash = 17 * hash + Objects.hashCode(this.workflowName);
        hash = 17 * hash + Objects.hashCode(this.name);
        return hash;
    }
}
