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
    private int dbid;
    private char status = 'W';
    private Task start;
    private Task end;
    private ArrayList<Task> tasks = new ArrayList<>();
    private DirectedSparseGraph<Task, String> graph = new DirectedSparseGraph<>();
    private String name = "";
    private HashSet<WorkflowFile> inputFiles = new HashSet<>();
    private HashSet<WorkflowFile> outputFiles = new HashSet<>();

    public Workflow(String name) throws DBException
    {
        this.name = name;
        dbid = new DBRecord("workflow", "name", name, "status", status, "submitted", Utils.time()).insert();
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
    
    public void addStartAndEndTask() throws DBException
    {
        //Add START and END task if neccessary
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
            start = Task.getWorkflowTask("START", 0, this, "./dummy;0", "Dummy");
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
            end = Task.getWorkflowTask("END", 0, this, "./dummy;0", "Dummy");
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
//                    double runtime = Double.parseDouble(jobElement.getAttribute("runtime"));//                    double runtime = Double.parseDouble(jobElement.getAttribute("runtime"));
                    double runtime = 1;

                    String taskName = jobElement.getAttribute("name");
//                    String taskNameSpace = jobElement.getAttribute("namespace");
                    Task task = Task.getWorkflowTask(idString+taskName, runtime, wf, "", namespace);
                    task.addInputFile(WorkflowFile.getFile(taskName, 1, WorkflowFile.TYPE_FILE));
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
        wf.addStartAndEndTask();
        wf.generateFileSet();
        wf.prepareWorkingDirectory();
//        wf.createDummyInputFiles();
        wf.save(Utils.getProp("working_dir")+wf.dbid+"/"+wf.dbid+".wfobj");
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
            String outfile = Utils.getProp("working_dir")+dbid+"/"+r.get("name");
            try{
            Process p = Runtime.getRuntime().exec(new String[]
            {
                "/bin/bash", "-c", "dd if=/dev/zero of="+outfile+" bs=8k count="+(int)Math.round(r.getDouble("estsize")/8192/1024)+" 2>&1 >/dev/null"
            });
            p.waitFor();
            }
            catch(IOException | InterruptedException ex)
            {
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
                String type = f.getType() == WorkflowFile.TYPE_DIRECTIORY ? "dir" : "file";
                pw.printf("\t\t<uses "
                        + "type=\"%s\" "
                        + "file=\"%s\" "
                        + "link=\"input\" "
                        + "size=\"%f\"/>\n"
                        ,type, f.getName(), f.getSize());
            }
            for(WorkflowFile f : t.getOutputFiles())
            {
                String type = f.getType() == WorkflowFile.TYPE_DIRECTIORY ? "dir" : "file";
                pw.printf("\t\t<uses "
                        + "type=\"%s\" "
                        + "file=\"%s\" "
                        + "link=\"output\" "
                        + "size=\"%f\"/>\n"
                        ,type, f.getName(), f.getSize());
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
    
    
    public static void main(String[] args) throws DBException, FileNotFoundException
    {
        Utils.disableDB();
        Workflow wf = fromDAX("/home/orachun/Desktop/dag.xml");
        
    }
}
