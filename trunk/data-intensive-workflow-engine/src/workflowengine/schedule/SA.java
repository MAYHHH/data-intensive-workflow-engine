package workflowengine.schedule;

import static java.lang.Math.exp;
import java.util.Random;
import workflowengine.utils.Utils;

public class SA implements Scheduler
{
    static double CHANGE_FACTOR;
    static int START_TEMP = 100;
    static int INNER_COUNT = 10;
    static int DECREASE_RATE = 1;
    static final Random r = new Random();
    
    static Schedule bestSolution;
    static double bestFit = Double.POSITIVE_INFINITY;
    
    public SA()
    {
        if(Utils.hasProp("CHANGE_FACTOR"))
        {
            CHANGE_FACTOR = Utils.getDoubleProp("CHANGE_FACTOR");
        }
        else
        {
            CHANGE_FACTOR = 0.05;
        }
        if(Utils.hasProp("START_TEMP"))
        {
            START_TEMP = Utils.getIntProp("START_TEMP");
        }
        else
        {
            START_TEMP = 100;
        }
        if(Utils.hasProp("INNER_COUNT"))
        {
            INNER_COUNT = Utils.getIntProp("INNER_COUNT");
        }
        else
        {
            INNER_COUNT = 10;
        }
        if(Utils.hasProp("DECREASE_RATE"))
        {
            DECREASE_RATE = Utils.getIntProp("DECREASE_RATE");
        }
        else
        {
            DECREASE_RATE = 1;
        }
    }
    
    @Override
    public Schedule getSchedule(SchedulerSettings settings)
    {
        bestSolution = null;
        int curTemp;
        double curFit;
        Schedule solution ;
        if(settings.hasParam("init_schedule"))
        {
            solution = (Schedule)settings.getObjectParam("init_schedule");
        }
        else
        {
            solution = new Schedule(settings);
            solution.random();
        }
        curFit = fitFunc(solution);
        bestFit = curFit;
        bestSolution = solution.copy();
        for (curTemp = START_TEMP; curTemp > 0; curTemp -= DECREASE_RATE)
        {
            for (int i = 0; i < INNER_COUNT; i++)
            {
                Schedule tempSolution = solution.copy();
                double tempFit;
                slightChange(tempSolution, settings);
                tempFit = fitFunc(tempSolution);
                if (tempFit < bestFit)
                {
                    bestFit = tempFit;
                    bestSolution = tempSolution.copy();
                }
                //printSol(tempSolution);
                //System.out.println(tempFit);
                if ((tempFit < curFit) || (r.nextDouble() < bolzMan(tempFit - curFit, curTemp)))
                {
                    curFit = tempFit;
                    solution = tempSolution.copy();
                }
            }
            System.out.println(bestFit);
        }
        bestSolution.evaluate();
        return bestSolution;
    }

    double bolzMan(double delta, double temp)
    {
        double prob = exp(-delta / temp);
        return prob;
    }

    double randomP(double pmin, double pmax)
    {
        double interval = pmax - pmin;
        double value = r.nextDouble() * interval;
        return pmin + value;
    }

    double fitFunc(Schedule solution)
    {
//        double fitness = 0;
//        if (solution.getMakespan() > DEADLINE)
//        {
//            double PenaltyFunction = CONSTANT_PERNALTY + WEIGHT * (solution.getMakespan() - DEADLINE);
//            fitness = solution.getCost() + PenaltyFunction;
//        }
//        else
//        {
//            fitness = solution.getCost();
//        }
//        return fitness;
        return solution.getFitness();
    }

    void slightChange(Schedule sch, SchedulerSettings ss)
    {
        for (int k = 0; k < ss.getTotalTasks(); k++)
        {

            int wkId = ss.getWorkerIndex(sch.getWorkerForTask(k));
            if (r.nextBoolean())
            {
                wkId++;
                wkId = Math.min(wkId, ss.getTotalWorkers() - 1);
            }
            else
            {
                wkId--;
                wkId = Math.max(wkId, 0);
            }
            sch.setWorkerForTask(k, wkId);
        }
    }
}