/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.workflow;

import edu.uci.ics.jung.graph.DirectedSparseGraph;
import edu.uci.ics.jung.graph.util.EdgeType;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Collection;
import java.util.LinkedList;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import workflowengine.TaskManager;
import workflowengine.utils.DBException;
import workflowengine.utils.DBRecord;
import workflowengine.utils.Utils;
import workflowengine.utils.XMLUtils;

/**
 *
 * @author Orachun
 */
public class Workflow implements Serializable
{
    public static final char STATUS_SUBMITTED = 'W';
    public static final char STATUS_SCHEDULED = 'S';
    public static final char STATUS_COMPLETED = 'C';
    
    public static final double AVG_WORKLOAD = 10;
    public static final double AVG_FILE_SIZE = 3 * Utils.MB;
    
    private int dbid;
    private char status = STATUS_SUBMITTED;
//    private Task start;
//    private Task end;
    private List<Task> start;
    private List<Task> end;
    private ArrayList<Task> tasks = new ArrayList<>();
    private DirectedSparseGraph<Task, String> graph = new DirectedSparseGraph<>();
    private String name = "";
    private HashSet<WorkflowFile> inputFiles = new HashSet<>();
    private HashSet<WorkflowFile> outputFiles = new HashSet<>();
    private boolean insertToDB;

    public Workflow(String name) throws DBException
    {
        this(name, true);
    }
    public Workflow(String name, boolean insertToDB)
    {
        this.name = name;
        this.insertToDB = insertToDB;
        if(insertToDB)
        {
            dbid = new DBRecord("workflow", "name", name, "status", status, "submitted", Utils.time()).insert();
        }
    }
    public int getTaskIndex(Task t)
    {
        return tasks.indexOf(t);
    }
    public Task getTask(int index)
    {
        return tasks.get(index);
    }
    public static void setStartedTime(int wfid, long time)
    {
        DBRecord.update("UPDATE workflow SET started_at='"+time+"' WHERE wfid='"+wfid+"'");
        setStatus(wfid, STATUS_SUBMITTED);
    }
    
    public static void setScheduledTime(int wfid, long time)
    {
        DBRecord.update("UPDATE workflow SET scheduled_at='"+time+"' WHERE wfid='"+wfid+"'");
        setStatus(wfid, STATUS_SCHEDULED);
    }
    public static void setFinishedTime(int wfid, long time)
    {
        DBRecord.update("UPDATE workflow SET finished_at='"+time+"' WHERE wfid='"+wfid+"'");
        setStatus(wfid, STATUS_COMPLETED);
    }
    public static void setEstimatedFinishTime(int wfid, long time)
    {
        DBRecord.update("UPDATE workflow SET est_finish='"+time+"' WHERE wfid='"+wfid+"'");
        setStatus(wfid, STATUS_COMPLETED);
    }
    public static int getEstimatedFinishTime(int wfid)
    {
        try
        {
            return DBRecord.select("SELECT est_finish FROM workflow WHERE wfid='"+wfid+"'").get(0).getInt("est_finish");
        }
        catch(IndexOutOfBoundsException e)
        {
            return -1;
        }
    }
    public static void setStatus(int wfid, char status)
    {
        DBRecord.update("UPDATE workflow SET status='"+status+"' WHERE wfid='"+wfid+"'");
    }
    public static boolean isFinished(int wfid)
    {
        return DBRecord.select("SELECT count(*) AS incompleted_tasks "
                + "FROM workflow_task "
                + "WHERE wfid='"+wfid+"' "
                + "AND status<>'C'").get(0).getInt("incompleted_tasks") == 0;
    }
    
    public void prepareWorkingDirectory()
    {
        new File(Utils.getProp("working_dir")+dbid).mkdir();
        try
        {
            Runtime.getRuntime().exec(new String[]{
                "/bin/bash", "-c", "cp "+ Utils.getProp("working_dir")+"dummy "+
                Utils.getProp("working_dir")+dbid+"/"
            }).waitFor();
        }
        catch (IOException | InterruptedException ex)
        {
            TaskManager.logger.log("Cannot prepare working directory");
        }
    }
    
    @Override
    public String toString()
    {
        return name;
    }
    @Override
    public boolean equals(Object o)
    {
        return o instanceof Workflow && this.toString().equals(o.toString());
    }

    @Override
    public int hashCode()
    {
        int hash = 5;
        hash = 53 * hash + Objects.hashCode(this.name);
        return hash;
    }
    public int getDbid()
    {
        return dbid;
    }

    public int getTotalTasks()
    {
        return tasks.size();
    }

//    public Task getTask(int i)
//    {
//        return tasks.get(i);
//    }

    public String getName()
    {
        return name;
    }

    public void addTask(Task t)
    {
        tasks.add(t);
        graph.addVertex(t);
    }
    
    public void addEdge(Task from, Task to) throws DBException
    {
        if (!tasks.contains(from))
        {
            tasks.add(from);
        }
        if (!tasks.contains(to))
        {
            tasks.add(to);
        }
        graph.addEdge(from.getEdgeName(to), from, to, EdgeType.DIRECTED);
        
        if(insertToDB)
        {
            new DBRecord("workflow_task_depen", "wfid", dbid, "parent", from.getDbid(), "child", to.getDbid()).insertIfNotExist();
        }
    }
    

    public void generateFileSet()
    {
        HashSet<WorkflowFile> newInputFiles = new HashSet<>(inputFiles);
        for(WorkflowFile f : inputFiles)
        {
            if(outputFiles.contains(f))
            {
                newInputFiles.remove(f);
                outputFiles.remove(f);
            }
        }
        inputFiles = newInputFiles;
    }
    
    public void defineStartAndEndTasks() throws DBException
    {
        //Add START and END task if neccessary
        start = new LinkedList<>();
        end = new LinkedList<>();
        for (Task t : tasks)
        {
            if (graph.getInEdges(t).isEmpty())
            {
                start.add(t);
            }
            if (graph.getOutEdges(t).isEmpty())
            {
                end.add(t);
            }
        }
//        if (start.size() == 1)
//        {
//            start = start.get(0);
//        }
//        else
//        {
//            start = Task.getWorkflowTask("START", 0, this, "./dummy;0", "Dummy", insertToDB);
//            for (Task t : start)
//            {
//                addEdge(start, t);
//            }
//        }
//        if (end.size() == 1)
//        {
//            end = end.get(0);
//        }
//        else
//        {
//            end = Task.getWorkflowTask("END", 0, this, "./dummy;0", "Dummy", insertToDB);
//            for (Task t : end)
//            {
//                addEdge(t, end);
//            }
//        }
//        
    }

    public List<Task> getStartTasks()
    {
//        return start;
        return new ArrayList<>(start);
    }

    public List<Task> getTaskList()
    {
        return new ArrayList<>(tasks);
    }
    
    public List<Task> getEndTasks()
    {
//        return end;
        return new ArrayList<>(end);
    }

    public Collection<Task> getChildTasks(Task t)
    {
        return graph.getSuccessors(t);
    }

    public Collection<Task> getParentTasks(Task t)
    {
        return graph.getPredecessors(t);
    }

    
    public static Workflow fromDAX(String filename) throws DBException
    {
        return fromDAX(filename, false);
    }
    
    public static Workflow fromDAX(String filename, boolean forSim) throws DBException
    {
        File f = new File(filename);
        Workflow wf = new Workflow(f.getName(), !forSim);
        try
        {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document dom = db.parse(filename);
            Element docEle = dom.getDocumentElement();
            String namespace = docEle.getAttribute("name");
            NodeList jobNodeList = docEle.getElementsByTagName("job");

            HashMap<Integer, Task> tasks = new HashMap<>();
            if (jobNodeList != null && jobNodeList.getLength() > 0)
            {
                for (int i = 0; i < jobNodeList.getLength(); i++)
                {
                    Element jobElement = (Element) jobNodeList.item(i);
                    
                    String idString = jobElement.getAttribute("id");
                    int id = Integer.parseInt(idString.substring(2));
//                    double runtime = Double.parseDouble(jobElement.getAttribute("runtime"));//   

                    String taskName = jobElement.getAttribute("name");                 
                    double runtime = Task.getRecordedExecTime(wf.getName(), idString+taskName);
                    if(runtime == -1)
                    {
                        runtime = AVG_WORKLOAD;
                    }
//                    String taskNameSpace = jobElement.getAttribute("namespace");
                    Task task = Task.getWorkflowTask(idString+taskName, runtime, wf, "", namespace);
                    task.addInputFile(WorkflowFile.getFile(taskName, AVG_FILE_SIZE, WorkflowFile.TYPE_FILE));
                    StringBuilder cmdBuilder = new StringBuilder();
                    cmdBuilder.append("./dummy;").append(runtime).append(";");
                    tasks.put(id, task);

                    NodeList fileNodeList = jobElement.getElementsByTagName("uses");
                    for (int j = 0; j < fileNodeList.getLength(); j++)
                    {
                        Element fileElement = (Element) fileNodeList.item(j);
//                        String fname = fileElement.getAttribute("file");
                        String fname = fileElement.getAttribute("name");
                        String fiotype = fileElement.getAttribute("link");
//                        char ftype = fileElement.getAttribute("type").equals("dir") ? WorkflowFile.TYPE_DIRECTIORY:WorkflowFile.TYPE_FILE;
                        char ftype = WorkflowFile.TYPE_FILE;

//                        double fsize = Double.parseDouble(fileElement.getAttribute("size"));
                        double fsize = 1;
                        WorkflowFile wfile = WorkflowFile.getFile(fname, fsize, ftype);
                        if (fiotype.equals("input"))
                        {
                            cmdBuilder.append("i;");
                            task.addInputFile(wfile);
                            wf.inputFiles.add(wfile);
                        }
                        else
                        {
                            cmdBuilder.append("o;");
                            task.addOutputFile(wfile);
                            wf.outputFiles.add(wfile);
                        }
                        cmdBuilder.append(fname).append(";");
                        cmdBuilder.append(fsize).append(";");
                    }
                    cmdBuilder.deleteCharAt(cmdBuilder.length()-1);
                    
//                    String cmd = jobElement.getAttribute("cmd");
                    String cmd = XMLUtils.argumentTagToCmd(jobElement);
                    if(cmd == null)
                    {
                        task.setCmd(cmdBuilder.toString());
                    }
                    else
                    {
                        task.setCmd(cmd);
                    }
                    wf.addTask(task);
                }
            }

            
            //Read dependencies
            jobNodeList = docEle.getElementsByTagName("child");
            if (jobNodeList != null && jobNodeList.getLength() > 0)
            {
                for (int i = 0; i < jobNodeList.getLength(); i++)
                {
                    Element el = (Element) jobNodeList.item(i);
                    String refString = el.getAttribute("ref");
                    int childRef = Integer.parseInt(refString.substring(2));
                    Task child = tasks.get(childRef);
                    NodeList parents = el.getElementsByTagName("parent");
                    if (parents != null && parents.getLength() > 0)
                    {
                        for (int j = 0; j < parents.getLength(); j++)
                        {
                            el = (Element) parents.item(j);
                            String parentRefString = el.getAttribute("ref");
                            int parentRef = Integer.parseInt(parentRefString.substring(2));
                            Task parent = tasks.get(parentRef);
                            wf.addEdge(parent, child);
                        }
                    }
                }
            }
        }
        catch (ParserConfigurationException | SAXException | IOException | NumberFormatException e)
        {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
        wf.defineStartAndEndTasks();
        wf.generateFileSet();
        
        if(!forSim)
        {
            wf.prepareWorkingDirectory();
    //        wf.createDummyInputFiles();
            wf.save(Utils.getProp("working_dir")+wf.dbid+"/"+wf.dbid+".wfobj");
        }
        
        System.gc();
        return wf;
    }
    
    
    public static Workflow fromDummyDAX(String filename, boolean forSim)
    {
        File f = new File(filename);
        Workflow wf = new Workflow(f.getName(), !forSim);
        try
        {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document dom = db.parse(filename);
            Element docEle = dom.getDocumentElement();
            String namespace = "Dummy";
            NodeList jobNodeList = docEle.getElementsByTagName("job");

            HashMap<Integer, Task> tasks = new HashMap<>();
            if (jobNodeList != null && jobNodeList.getLength() > 0)
            {
                for (int i = 0; i < jobNodeList.getLength(); i++)
                {
                    Element jobElement = (Element) jobNodeList.item(i);
                    
                    String idString = jobElement.getAttribute("id");
                    int id = Integer.parseInt(idString.substring(2));
//                    double runtime = Double.parseDouble(jobElement.getAttribute("runtime"));//   

                    String taskName = jobElement.getAttribute("name");      
                    double runtime = Math.ceil(Double.parseDouble(jobElement.getAttribute("runtime")));
                    Task task = Task.getWorkflowTask(idString+taskName, runtime, wf, "", namespace);
//                    task.addInputFile(WorkflowFile.getFile("dummy.sh", AVG_FILE_SIZE, WorkflowFile.TYPE_FILE));
                    StringBuilder cmdBuilder = new StringBuilder();
                    cmdBuilder.append("dummy.sh;").append(runtime).append(";");
                    tasks.put(id, task);

                    NodeList fileNodeList = jobElement.getElementsByTagName("uses");
                    for (int j = 0; j < fileNodeList.getLength(); j++)
                    {
                        Element fileElement = (Element) fileNodeList.item(j);
                        String fname = fileElement.getAttribute("file");
                        String fiotype = fileElement.getAttribute("link");
                        char ftype = WorkflowFile.TYPE_FILE;

                        double fsize = 1+Math.round(Double.parseDouble(fileElement.getAttribute("size"))*Utils.BYTE);
                        WorkflowFile wfile = WorkflowFile.getFile(fname, fsize, ftype);
                        if (fiotype.equals("input"))
                        {
                            cmdBuilder.append("i;");
                            task.addInputFile(wfile);
                            wf.inputFiles.add(wfile);
                        }
                        else
                        {
                            cmdBuilder.append("o;");
                            task.addOutputFile(wfile);
                            wf.outputFiles.add(wfile);
                        }
                        cmdBuilder.append(fname).append(";");
                        cmdBuilder.append((int)fsize).append(";");
                    }
                    cmdBuilder.deleteCharAt(cmdBuilder.length()-1);
                    task.setCmd(cmdBuilder.toString());
                    wf.addTask(task);
                }
            }

            
            //Read dependencies
            jobNodeList = docEle.getElementsByTagName("child");
            if (jobNodeList != null && jobNodeList.getLength() > 0)
            {
                for (int i = 0; i < jobNodeList.getLength(); i++)
                {
                    Element el = (Element) jobNodeList.item(i);
                    String refString = el.getAttribute("ref");
                    int childRef = Integer.parseInt(refString.substring(2));
                    Task child = tasks.get(childRef);
                    NodeList parents = el.getElementsByTagName("parent");
                    if (parents != null && parents.getLength() > 0)
                    {
                        for (int j = 0; j < parents.getLength(); j++)
                        {
                            el = (Element) parents.item(j);
                            String parentRefString = el.getAttribute("ref");
                            int parentRef = Integer.parseInt(parentRefString.substring(2));
                            Task parent = tasks.get(parentRef);
                            wf.addEdge(parent, child);
                        }
                    }
                }
            }
        }
        catch (ParserConfigurationException | SAXException | IOException | NumberFormatException e)
        {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
        wf.defineStartAndEndTasks();
        wf.generateFileSet();
        
        if(!forSim)
        {
            wf.prepareWorkingDirectory();
//            wf.createDummyInputFiles();
            wf.save(Utils.getProp("working_dir")+wf.dbid+"/"+wf.dbid+".wfobj");
        }
        
        System.gc();
        return wf;
    }
    
    
    public Iterable<Task> getTaskIterator()
    {
        return new Iterable<Task>(){
            @Override
            public Iterator<Task> iterator()
            {
                return tasks.iterator();
            }
        };
    }

    public void print()
    {
        System.out.println(name);
        for (Task p : tasks)
        {
            for (String e : graph.getOutEdges(p))
            {
                System.out.println("  " + p.toString() + " -> " + graph.getOpposite(p, e).toString());
            }
        }
    }
    
    public void createDummyInputFiles(String destDir)
    {
        
        for(WorkflowFile f : this.inputFiles)
        {
            String outfile = destDir + f.getName();
            File file = new File(outfile);
            file.getParentFile().mkdirs();
            try
            {
                file.createNewFile();
                Process p = Runtime.getRuntime().exec(new String[]
                {
                    "/bin/bash", "-c", "fallocate -l " + ((int) Math.round(f.getSize())*1024*1024) + " "+outfile
                });
                p.waitFor();
            }
            catch(IOException | InterruptedException ex)
            {
                System.out.println(ex.getMessage());
                TaskManager.logger.log("Exception while creating a dummy input file: "+ex.getMessage());
            }
        }
    }
    
    public List<WorkflowFile> getInputFiles()
    {
        return new ArrayList(inputFiles);
    }
    
    public void save(String filename)
    {
        try
        {
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filename));
            oos.writeObject(this);
            oos.close();
        }
        catch(IOException ex)
        {
            TaskManager.logger.log("Cannot save the workflow object: "+ex.getMessage());
        }
    }
    
    public static Workflow open(String filename)
    {
        try
        {
            FileInputStream fin = new FileInputStream(filename);
            ObjectInputStream ois = new ObjectInputStream(fin);
            Workflow wf = (Workflow) ois.readObject();
            ois.close();
            return wf;
        }
        catch(ClassNotFoundException | IOException ex)
        {
            TaskManager.logger.log("Cannot read the workflow object: "+ex.getMessage());
        }
        return null;
    }
    
    public String getWorkingDirSuffix()
    {
        return "wf_"+dbid+"/";
    }
    
    public void toFile(String filename) throws FileNotFoundException
    {
        PrintWriter pw = new PrintWriter(filename);
        pw.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        HashMap<Task, Integer> taskMap = new HashMap<>(tasks.size());
        for(int i=0;i<tasks.size();i++)
        {
            Task t = tasks.get(i);
            taskMap.put(t, i);
            pw.printf("\t<job id=\"ID%05d\" "
                    + "namespace=\"Montage\" "
                    + "name=\"%s\" "
                    + "cmd=\"%s\" "
                    + "version=\"1.0\" "
                    + "runtime=\"%f\">\n",
                    i, t.getName(), t.getCmd(), t.getOperations());
            for(WorkflowFile f : t.getInputFiles())
            {
//                String type = f.getType() == WorkflowFile.TYPE_DIRECTIORY ? "dir" : "file";
                pw.printf("\t\t<uses "
//                        + "type=\"%s\" "
                        + "file=\"%s\" "
                        + "link=\"input\" "
                        + "size=\"%f\"/>\n"
//                        ,type
                        , f.getName(), f.getSize());
            }
            for(WorkflowFile f : t.getOutputFiles())
            {
//                String type = f.getType() == WorkflowFile.TYPE_DIRECTIORY ? "dir" : "file";
                pw.printf("\t\t<uses "
//                        + "type=\"%s\" "
                        + "file=\"%s\" "
                        + "link=\"output\" "
                        + "size=\"%f\"/>\n"
//                        ,type
                        , f.getName(), f.getSize());
            }
            pw.println("\t</job>");
        }
        for(int i=0;i<tasks.size();i++)
        {
            Task t = tasks.get(i);
            Collection<Task> parents = getParentTasks(t);
            if(parents.isEmpty())
            {
                continue;
            }
            pw.printf("\t<child ref=\"ID%05d\">\n", taskMap.get(t));
            for(Task p : parents)
            {
                pw.printf("\t\t<parent ref=\"ID%05d\"/>\n", taskMap.get(p));
            }
            pw.println("\t</child>");
        }
        pw.println("</adag>");
        pw.close();
    }
    
    /**
     * Return task queue ordered by the task dependency that the parent
     * task will come before the child task
     * @return 
     */
    public LinkedList<Task> getTaskQueue()
    {
        LinkedList<Task> taskQueue = new LinkedList<>();
        LinkedList<Task> q = new LinkedList<>();
        q.addAll(this.getEndTasks());
        while(!q.isEmpty())
        {
            Task t = q.poll();
            taskQueue.remove(t);
            taskQueue.push(t);
            Collection<Task> parents = this.getParentTasks(t);
            q.removeAll(parents);
            q.addAll(parents);
        }
        return taskQueue;
    }
    
    
    public static void main(String[] args) throws DBException, FileNotFoundException
    {
        Utils.disableDB();
        Workflow wf = fromDAX("/home/orachun/Desktop/dag.xml");
        
    }
}
