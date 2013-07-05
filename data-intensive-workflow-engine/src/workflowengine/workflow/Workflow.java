/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.workflow;

import edu.uci.ics.jung.graph.DirectedSparseGraph;
import edu.uci.ics.jung.graph.util.EdgeType;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Collection;
import java.util.LinkedList;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
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
import workflowengine.WorkflowEngine;
import static workflowengine.WorkflowEngine.PROP;
import workflowengine.resource.ExecSite;
import workflowengine.schedule.Schedule;
import workflowengine.utils.DBException;
import workflowengine.utils.DBRecord;
import workflowengine.utils.Utils;

/**
 *
 * @author Orachun
 */
public class Workflow implements Serializable
{
    private int dbid;
    private char status = 'W';
    private Task start;
    private Task end;
    private ArrayList<Task> tasks = new ArrayList<>();
    private DirectedSparseGraph<Task, String> graph = new DirectedSparseGraph<>();
    private String name = "";
//    private HashMap<String, WorkflowFile> inputFiles = new HashMap<>();
//    private HashMap<String, WorkflowFile> outputFiles = new HashMap<>();

    public Workflow(String name) throws DBException
    {
        this.name = name;
        dbid = new DBRecord("workflow", "name", name, "status", status, "submitted", Utils.time()).insert();
    }
    
    public void prepareWorkingDirectory()
    {
        new File(WorkflowEngine.PROP.getProperty("working_dir")+dbid).mkdir();
        try
        {
            Runtime.getRuntime().exec(new String[]{
                "/bin/bash", "-c", "cp "+ WorkflowEngine.PROP.getProperty("working_dir")+"dummy "+
                WorkflowEngine.PROP.getProperty("working_dir")+dbid+"/"
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
        
        new DBRecord("workflow_task_depen", "wfid", dbid, "parent", from.getDbid(), "child", to.getDbid()).insert();
    }

    //Add a start and an end tasks if needed
    public void finalizeWorkflow() throws DBException
    {
        LinkedList<Task> startTasks = new LinkedList<>();
        LinkedList<Task> endTasks = new LinkedList<>();
        for (Task t : tasks)
        {
            if (graph.getInEdges(t).isEmpty())
            {
                startTasks.add(t);
            }
            if (graph.getOutEdges(t).isEmpty())
            {
                endTasks.add(t);
            }
        }
        if (startTasks.size() == 1)
        {
            start = startTasks.get(0);
        }
        else
        {
            start = Task.getWorkflowTask("START", 0, this, "./dummy;0");
            for (Task t : startTasks)
            {
                addEdge(start, t);
            }
        }
        if (endTasks.size() == 1)
        {
            end = endTasks.get(0);
        }
        else
        {
            end = Task.getWorkflowTask("END", 0, this, "./dummy;0");
            for (Task t : endTasks)
            {
                addEdge(t, end);
            }
        }
    }

    public Task getStartTask()
    {
        return start;
    }

    public List<Task> getTaskList()
    {
        return new ArrayList<>(tasks);
    }
    
    public Task getEndTask()
    {
        return end;
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
        File f = new File(filename);
        Workflow wf = new Workflow(f.getName());
        try
        {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document dom = db.parse(filename);
            Element docEle = dom.getDocumentElement();
            NodeList nl = docEle.getElementsByTagName("job");

            HashMap<Integer, Task> tasks = new HashMap<>();
            if (nl != null && nl.getLength() > 0)
            {
                for (int i = 0; i < nl.getLength(); i++)
                {
                    Element el = (Element) nl.item(i);
                    String idString = el.getAttribute("id");
                    int id = Integer.parseInt(idString.substring(2));
                    double runtime = Double.parseDouble(el.getAttribute("runtime"));
                    String taskName = el.getAttribute("name");
                    Task task = Task.getWorkflowTask("("+idString+")"+taskName, runtime, wf, "");
                    StringBuilder cmd = new StringBuilder();
                    cmd.append("./dummy;").append(runtime).append(";");
                    tasks.put(id, task);

                    NodeList files = el.getElementsByTagName("uses");
                    for (int j = 0; j < files.getLength(); j++)
                    {
                        Element fel = (Element) files.item(j);
                        String fname = fel.getAttribute("file");
                        String ftype = fel.getAttribute("link");
                        double fsize = Double.parseDouble(fel.getAttribute("size"));
                        WorkflowFile wfile = WorkflowFile.getFile(fname, fsize);
                        if (ftype.equals("input"))
                        {
                            cmd.append("i;");
                            task.addInputFile(wfile);
                        }
                        else
                        {
                            cmd.append("o;");
                            task.addOutputFile(wfile);
                        }
                        cmd.append(fname).append(";");
                        cmd.append(fsize).append(";");
                    }
                    cmd.deleteCharAt(cmd.length()-1);
                    task.setCmd(cmd.toString());
                }
            }

            //Read dependencies
            nl = docEle.getElementsByTagName("child");
            if (nl != null && nl.getLength() > 0)
            {
                for (int i = 0; i < nl.getLength(); i++)
                {
                    Element el = (Element) nl.item(i);
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
            throw new RuntimeException(e.getMessage());
        }
        wf.finalizeWorkflow();
        wf.prepareWorkingDirectory();
        wf.createDummyInputFiles();
        wf.save(WorkflowEngine.PROP.getProperty("working_dir")+wf.dbid+"/"+wf.dbid+".wfobj");
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
    
    public void createDummyInputFiles()
    {
        List<DBRecord> results = DBRecord.select("_workflow_input_file", new DBRecord("_workflow_input_file", "wfid", dbid));
        for(DBRecord r : results)
        {
            String outfile = WorkflowEngine.PROP.getProperty("working_dir")+dbid+"/"+r.get("name");
            try{
            Process p = Runtime.getRuntime().exec(new String[]
            {
                "/bin/bash", "-c", "dd if=/dev/zero of="+outfile+" bs=8k count="+(int)Math.round(r.getDouble("estsize")/8192)+" 2>&1 >/dev/null"
            });
            p.waitFor();
            }
            catch(IOException | InterruptedException ex)
            {
                TaskManager.logger.log("Exception while creating a dummy input file: "+ex.getMessage());
            }
        }
    }
    
    public void save(String filename)
    {
        try
        {
            FileOutputStream fout = new FileOutputStream(filename);
            ObjectOutputStream oos = new ObjectOutputStream(fout);
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
    
    

    public static void main(String[] args)
    {
        try{
        PROP.setProperty("DBHost", "10.217.165.63");
        PROP.setProperty("DBPort", "6612");
        PROP.setProperty("DBName", "workflow_engine");
        PROP.setProperty("DBUser", "root");
        PROP.setProperty("DBPass", "1234");
        
        Workflow wf = Workflow.fromDAX("../ExampleDAGs/Simple_5.xml");
        wf.print();
        ExecSite nw = ExecSite.random(2);
        Schedule s = new Schedule(wf, nw);
        s.random();
        s.getMakespan();
        s.print();
        }catch(DBException ex)
        {
            System.err.println(ex.getMessage());
        }
    }
}
