package workflowengine.workflow;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import workflowengine.utils.DBException;
import workflowengine.utils.DBRecord;
import workflowengine.utils.Utils;

/**
 *
 * @author Orachun
 */
public class Task implements Serializable, Comparable<Task>
{

    public static final char STATUS_WAITING = 'W';
    public static final char STATUS_DISPATCHED = 'D';
    public static final char STATUS_EXECUTING = 'E';
    public static final char STATUS_COMPLETED = 'C';
    public static final char STATUS_FAIL = 'F';
    public static final char STATUS_SUSPENDED = 'S';
    private static int count = 0;
    private int id;
    private String workflowName;
    private int wfdbid;
    private String name = "";
    private double operations;//Represent estimated execution time
    private HashMap<String, Object> objProps = new HashMap<>();
    private int dbid = -1;
    private char status = STATUS_WAITING;
    private String namespace;
    private String cmd;
    private LinkedList<WorkflowFile> inputs = null;
    private LinkedList<WorkflowFile> outputs = null;

    private Task(String name, double operations, Workflow wf, String cmd, String namespace)
    {
        this.name = name;
        this.operations = operations;
        id = ++count;
        this.workflowName = wf.getName();
        this.wfdbid = wf.getDbid();
        this.cmd = cmd;
        this.namespace = namespace;
    }

    private Task(String name, double operations, String workflowName, int wfdbid, String cmd, String namespace, int dbid)
    {
        this.name = name;
        this.workflowName = workflowName;
        this.wfdbid = wfdbid;
        this.operations = operations;
        this.dbid = dbid;
        this.cmd = cmd;
        this.namespace = namespace;
    }

    /**
     * Return a dummy task which cannot do anything. This task will not be saved
     * into the database.
     *
     * @param name
     * @param wf
     * @return
     */
    public static Task getDummyTask(String name, Workflow wf)
    {
        Task t = new Task(name, 0, wf, "", "");
        return t;
    }

    public static Task getWorkflowTask(String name, double operations, Workflow wf, String cmd, String namespace)
    {
        return getWorkflowTask(name, operations, wf, cmd, namespace, true);
    }

    public static Task getWorkflowTask(String name, double operations, Workflow wf, String cmd, String namespace, boolean insert)
    {
        Task t = null;
        if (Utils.isDBEnabled())
        {
            t = getWorkflowTaskFromDB(name, wf.getName());
        }
        if (t == null)
        {
            t = new Task(name, operations, wf, cmd, namespace);
            if (insert)
            {
                t.insert();
            }
        }
        return t;
    }

    public static Task getWorkflowTaskFromDB(String name, String wfname)
    {
        if (!Utils.isDBEnabled())
        {
            throw new RuntimeException("Database is disabled");
        }
        try
        {
            DBRecord r = DBRecord.select("workflow_task",
                    "SELECT t.name, t.estopr, t.wfid, w.name as wname, t.tid, t.cmd, t.status, t.namespace "
                    + " FROM workflow_task t JOIN workflow w ON t.wfid = w.wfid "
                    + " WHERE t.name='" + wfname + ":" + name + "'").get(0);
            Task t = new Task(
                    r.get("name"),
                    r.getDouble("estopr"),
                    r.get("wname"),
                    r.getInt("wfid"),
                    r.get("cmd"),
                    r.get("namespace"),
                    r.getInt("tid"));
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
        if (!Utils.isDBEnabled())
        {
            throw new RuntimeException("Database is disabled");
        }
        try
        {
            DBRecord r = DBRecord.select("workflow_task",
                    "SELECT t.name, t.estopr, t.wfid, w.name as wname, t.tid, t.cmd, t.status, t.namespace "
                    + " FROM workflow_task t JOIN workflow w ON t.wfid = w.wfid "
                    + " WHERE t.tid='" + dbid + "'").get(0);
            Task t = new Task(
                    r.get("name"),
                    r.getDouble("estopr"),
                    r.get("wname"),
                    r.getInt("wfid"),
                    r.get("cmd"),
                    r.get("namespace"),
                    r.getInt("tid"));
            t.status = r.get("status").charAt(0);
            return t;
        }
        catch (IndexOutOfBoundsException ex)
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
        DBRecord.update("UPDATE workflow_task SET cmd='" + cmd + "' WHERE tid='" + dbid + "'");
    }

    public String getCmd()
    {
        return cmd;
    }

    public void setCheckpointFile(WorkflowFile f)
    {
        setCmd(f.getName());
    }

    public void insert()
    {
        if (dbid == -1)
        {
            DBRecord rec = new DBRecord("workflow_task",
                    "wfid", wfdbid,
                    "name", toString(),
                    "status", status,
                    "estopr", operations,
                    "cmd", cmd,
                    "namespace", namespace);
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
        if(status == STATUS_COMPLETED)
        {
            recordExecTime(tid, end-start);
        }
        return DBRecord.update("UPDATE workflow_task "
                + "SET start='" + start + "', finish='" + end + "', exit_value='" + exitValue + "', status='" + status + "' "
                + "WHERE tid='" + tid + "'");
    }

    private static void recordExecTime(int tid, long time)
    {
        Task t = Task.getWorkflowTaskFromDB(tid);
        new DBRecord("task_exec_time", 
                "wfname", t.workflowName,
                "tname", t.getName(),
                "exec_time", time).insert();
    }
    
    public static int getRecordedExecTime(String wfname, String tname)
    {
        if(!Utils.isDBEnabled())
        {
            return -1;
        }
        try
        {
            DBRecord r = DBRecord.select("SELECT AVG(exec_time) FROM task_exec_time "
                    + "WHERE wfname='"+wfname+"' "
                    + "AND tname='"+tname+"'").get(0);
            if(r.get("exec_time") == null)
            {
                return -1;
            }
            return (int)Math.round(r.getDouble("exec_time"));
        }
        catch(IndexOutOfBoundsException e)
        {
            return -1;
        }
    }
    
    public void addInputFile(WorkflowFile f) throws DBException
    {
        if (inputs == null)
        {
            inputs = new LinkedList<>();
        }
        inputs.add(f);
        new DBRecord("workflow_task_file", "type", "I", "tid", dbid, "fid", f.getDbid()).insert();
    }

    public void addOutputFile(WorkflowFile f) throws DBException
    {
        if (outputs == null)
        {
            outputs = new LinkedList<>();
        }
        outputs.add(f);
        new DBRecord("workflow_task_file", "type", "O", "tid", dbid, "fid", f.getDbid()).insert();
    }

    public int getID()
    {
        return id;
    }

    public String getName()
    {
        return name;
    }

    public String getNamespace()
    {
        return namespace;
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

    public WorkflowFile[] getInputFiles() throws DBException
    {
        if (Utils.isDBEnabled())
        {
            if (inputs == null)
            {
                List<DBRecord> results = DBRecord.select("workflow_task_file",
                        new DBRecord("workflow_task_file",
                        "type", "I",
                        "tid", dbid));
                WorkflowFile[] files = new WorkflowFile[results.size()];
                for (int i = 0; i < results.size(); i++)
                {
                    WorkflowFile f = WorkflowFile.getFileFromDB(results.get(i).getInt("fid"));
                    files[i] = f;
                }
                return files;
            }
            else
            {
                return inputs.toArray(new WorkflowFile[inputs.size()]);
            }
        }
        else
        {
            return inputs.toArray(new WorkflowFile[inputs.size()]);
        }
    }

//    public String[] getInputFilesString() throws DBException
//    {
//        List<DBRecord> results = DBRecord.select(
//                "SELECT f.name "
//                + "FROM workflow_task_file tf "
//                + "JOIN file f on tf.fid = f.fid "
//                + "WHERE type='I' AND tid='"+dbid+"'");
//        
//        String[] files = new String[results.size()];
//        for (int i=0;i<results.size();i++)
//        {
//            files[i] = results.get(i).get("name");
//        }
//        return files;
//    }
    public WorkflowFile[] getOutputFiles() throws DBException
    {
        if (Utils.isDBEnabled())
        {
            if (outputs == null)
            {
                List<DBRecord> results = DBRecord.select("workflow_task_file",
                        new DBRecord("workflow_task_file",
                        "type", "O",
                        "tid", dbid));
                WorkflowFile[] files = new WorkflowFile[results.size()];
                for (int i = 0; i < results.size(); i++)
                {
                    WorkflowFile f = WorkflowFile.getFileFromDB(results.get(i).getInt("fid"));
                    files[i] = f;
                }
                return files;
            }
            else
            {
                return outputs.toArray(new WorkflowFile[outputs.size()]);
            }
        }
        else
        {
            return outputs.toArray(new WorkflowFile[outputs.size()]);
        }
    }

    public WorkflowFile[] getOutputFilesForTask(Task t)
    {
        WorkflowFile[] out = this.getOutputFiles();
        WorkflowFile[] in = t.getInputFiles();
        LinkedList<WorkflowFile> files = new LinkedList<>();
        for (int i = 0; i < out.length; i++)
        {
            for (int j = 0; j < in.length; j++)
            {
                if (out[i].equals(in[j]))
                {
                    files.add(out[i]);
                }
            }
        }
        return files.toArray(new WorkflowFile[]
        {
        });
    }

//    public String[] getOutputFilesString() throws DBException
//    {
//        List<DBRecord> results = DBRecord.select(
//                "SELECT f.name "
//                + "FROM workflow_task_file tf "
//                + "JOIN file f on tf.fid = f.fid "
//                + "WHERE type='O' AND tid='"+dbid+"'");
//        String[] files = new String[results.size()];
//        for (int i=0;i<results.size();i++)
//        {
//            files[i] = results.get(i).get("name");
//        }
//        return files;
//    }
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
        return name;
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

    public String getWorkingDirSuffix()
    {
        return "wf_" + wfdbid + "/";
    }

    @Override
    public int compareTo(Task o)
    {
        int h1 = hashCode();
        int h2 = o.hashCode();
        if (h1 < h2)
        {
            return -1;
        }
        if (h1 == h2)
        {
            return 0;
        }
        return 1;
    }
}
