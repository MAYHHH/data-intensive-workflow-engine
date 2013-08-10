/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule.gapso;

import java.util.HashMap;
import java.util.Random;
import workflowengine.resource.ExecSite;
import workflowengine.schedule.HEFTScheduler;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.Scheduler;
import workflowengine.schedule.SchedulerSettings;
import workflowengine.schedule.fc.CostOptimizationFC;
import workflowengine.utils.Utils;
import workflowengine.workflow.Workflow;

/**
 *
 * @author Dew
 */
public class PSO implements Scheduler
{
    public static final String VAR_GBEST = "VAR_GBEST";
    public static final String PROP_ITERATIONS = "pso_iterations";
    public static final String PROP_POP_SIZE = "pso_population_size";
    public static final String PROP_PBEST_WEIGHT = "pso_pbest_weight";
    public static final String PROP_GBEST_WEIGHT = "pso_gbest_weight";
    public static final String PROP_INERTIA_WEIGHT = "pso_inertia_weight";
    
    private static double ITERATIONS;
    private static int POP_SIZE;
    private PSOIndividual[] population;
    private HashMap<String, Object> globalVars;

    public void init()
    {
        //0.0391
        Utils.setPropIfNotExist(PROP_ITERATIONS, "100");
        Utils.setPropIfNotExist(PROP_POP_SIZE, "20");
        Utils.setPropIfNotExist(PROP_PBEST_WEIGHT, "0.4");
        Utils.setPropIfNotExist(PROP_GBEST_WEIGHT, "0.3");
        Utils.setPropIfNotExist(PROP_INERTIA_WEIGHT, "0.3");
        
        ITERATIONS = Utils.getIntProp(PROP_ITERATIONS);
        POP_SIZE = Utils.getIntProp(PROP_POP_SIZE);
        globalVars = new HashMap<>();
        globalVars.put(PSO.VAR_GBEST, null);
        population = new PSOIndividual[POP_SIZE];
    }
    
    public Schedule getSchedule(SchedulerSettings ss)
    {
        init();
        for (int i = 0; i < POP_SIZE; i++)
        {
            population[i] = new PSOIndividual(ss, globalVars);
            population[i].random();
        }
        for (int k = 0; k < ITERATIONS; k++)
        {
            for (int j = 0; j < POP_SIZE; j++)
            {
                population[j].calVelocity();
                population[j].updatePosition();
                population[j].updateFitness();
            }
//            System.out.println(((Schedule)globalVars.get(VAR_GBEST)).getFitness());
        }
        return (Schedule)globalVars.get(VAR_GBEST);
    }

    /**
     * upperbound is excluded
     *
     * @param taskUpperBound
     * @param taskLowerBound
     * @param resourceBoundLength
     * @param resourceLowerBound
     * @param ss
     * @return
     */
    public Schedule limitBound(int taskUpperBound, int taskLowerBound, int resourceBoundLength, SchedulerSettings ss)
    {
        init();
        ss.setParam("taskUpperBound", taskUpperBound);
        ss.setParam("taskLowerBound", taskLowerBound);
        ss.setParam("resourceBoundLength", resourceBoundLength);
        for (int i = 0; i < POP_SIZE; i++)
        {
            population[i] = new PSOIndividual(ss, globalVars);
            population[i].random();
        }
        for (int k = 0; k < ITERATIONS; k++)
        {
            for (int j = 0; j < POP_SIZE; j++)
            {
                population[j].calVelocity();
                population[j].updatePosition();
                population[j].updateFitness();
            }
        }
        return (Schedule)globalVars.get(VAR_GBEST);
    }
    public static void main(String[] args)
    {
        Utils.disableDB();
        Workflow wf = Workflow.fromDummyDAX("/home/orachun/WorkflowEngine/dummy-dags/Montage_100.xml.dummy", true);
        ExecSite es = ExecSite.generate(30);
        
        Utils.setPropIfNotExist(PROP_ITERATIONS, "100");
        Utils.setPropIfNotExist(PROP_POP_SIZE, "20");
        Utils.setPropIfNotExist(PROP_PBEST_WEIGHT, "0.1");
        Utils.setPropIfNotExist(PROP_GBEST_WEIGHT, "0.1");
        Utils.setPropIfNotExist(PROP_INERTIA_WEIGHT, "0.8");
        
        Scheduler PSO = new PSO();
        SchedulerSettings ss = new SchedulerSettings(wf, es, new CostOptimizationFC());
        
        double total = 0;
        for(int i=0;i<10;i++)
        {
            Schedule psos = PSO.getSchedule(ss);
            System.out.println(psos.getFitness());
            total += psos.getFitness();
        }
        System.out.println(total/10);
    }
}
