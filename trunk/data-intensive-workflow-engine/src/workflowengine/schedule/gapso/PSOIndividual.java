/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule.gapso;

import java.util.HashMap;
import java.util.Random;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.SchedulerSettings;
import workflowengine.utils.Utils;

/**
 *
 * @author Dew
 */
public class PSOIndividual extends Schedule
{
    private double[] position;
    private double[] velocity;
    private PSOIndividual pBest;
    private Random r = new Random();
    private static double PBEST_WEIGHT;
    private static double GBEST_WEIGHT;
    private static double INERTIA_WEIGHT;
    private HashMap<String, Object> globalVars;
    private int startTask;
    private int endTask;

    public PSOIndividual(SchedulerSettings settings, HashMap<String, Object> globalVars)
    {
        super(settings);
        this.globalVars = globalVars;
        position = new double[settings.getTotalTasks()];
        velocity = new double[settings.getTotalTasks()];
        pBest = null;
        if(settings.hasParam("taskLowerBound"))
        {
            startTask = settings.getIntParam("taskLowerBound");
            endTask = settings.getIntParam("taskUpperBound");
        }
        else
        {
            startTask = 0;
            endTask = settings.getTotalTasks();
        }
        PBEST_WEIGHT = Utils.getDoubleProp(PSO.PROP_PBEST_WEIGHT);
        GBEST_WEIGHT = Utils.getDoubleProp(PSO.PROP_GBEST_WEIGHT);
        INERTIA_WEIGHT = Utils.getDoubleProp(PSO.PROP_INERTIA_WEIGHT);
    }
    
    public PSOIndividual(PSOIndividual p)
    {
        super(p);
        this.globalVars = p.globalVars;
        position = new double[settings.getTotalTasks()];
        velocity = new double[settings.getTotalTasks()];
        System.arraycopy(p.position, 0, this.position, 0, this.position.length);
        System.arraycopy(p.velocity, 0, this.velocity, 0, this.velocity.length);
        this.pBest = p.pBest;
        this.startTask = p.startTask;
        this.endTask = p.endTask;
    }

    void calVelocity()
    {
        PSOIndividual gBest = (PSOIndividual)globalVars.get(PSO.VAR_GBEST);
        for (int i = startTask; i < endTask; i++)
        {
            velocity[i] = INERTIA_WEIGHT * velocity[i] 
                    + (PBEST_WEIGHT * r.nextDouble() * (pBest.position[i] - position[i])) 
                    + (GBEST_WEIGHT * r.nextDouble() * (gBest.position[i] - position[i]));
        }
    }

    void updatePosition()
    {
        
        for (int i = startTask; i < endTask; i++)
        {
            position[i] = position[i] + velocity[i];
        }
    }

    void updateFitness()
    {
        Schedule sch = (Schedule) settings.getObjectParam("Solution");
        if(settings.hasParam("taskLowerBound"))
        {
            int totalworker = settings.getIntParam("resourceBoundLength");
            for (int i = 0; i < settings.getTotalTasks(); i++)
            {
                if(i >= startTask && i < endTask)
                {
                    int workerID = (int) Math.abs(Math.floor(position[i])) % totalworker;         
                    int oldWorkerID = settings.getWorkerIndex(sch.getWorkerForTask(i));
                    workerID = (int) Math.floor(workerID + oldWorkerID - settings.getIntParam("resourceBoundLength") / 2.0);
                    workerID = Math.max(0, workerID);
                    workerID = Math.min(settings.getTotalWorkers()-1, workerID);

                    this.setWorkerForTask(i, settings.getWorker(workerID));
                }
                else
                {
                    this.setWorkerForTask(i, sch.getWorkerForTask(i));
                }
            }
        }
        else
        {
            int totalworker = settings.getTotalWorkers();
            for (int i = 0; i < settings.getTotalTasks(); i++)
            {
                int workerID = (int) Math.abs(Math.floor(position[i])) % totalworker;  
                this.setWorkerForTask(i, settings.getWorker(workerID));
            }
        }
        if (this.pBest == null || this.getFitness() < this.pBest.getFitness())
        {
            this.pBest = new PSOIndividual(this);
        }
        
//        double fitnessgBest = (Double)globalVars.get(PSO.VAR_GBEST_FITNESS);
        PSOIndividual gBest = (PSOIndividual)globalVars.get(PSO.VAR_GBEST);
        
//        double[] gBestPosition = (double[])globalVars.get(PSO.VAR_GBEST_POSITION);
        if (gBest == null || this.getFitness() < gBest.getFitness())
        {
            globalVars.put(PSO.VAR_GBEST, new PSOIndividual(this));
        }
    }

    @Override
    public void random()
    {
        
        for (int i = startTask; i < endTask; i++)
        {
            position[i] = r.nextDouble() * settings.getTotalWorkers();
            velocity[i] = r.nextDouble() * settings.getTotalWorkers();
        }
        updateFitness();
    }
}
