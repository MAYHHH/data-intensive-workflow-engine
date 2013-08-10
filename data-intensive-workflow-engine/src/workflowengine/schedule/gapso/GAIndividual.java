/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule.gapso;

import static workflowengine.schedule.gapso.GA.r;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.SchedulerSettings;

/**
 *
 * @author Dew
 */
public class GAIndividual extends Schedule
{
    final static double C = 0.3;
    final static double WEIGHT = 0.5;
//    private double fitness = 0;

    public GAIndividual(SchedulerSettings settings)
    {
        super(settings);
    }

    public GAIndividual(Schedule s)
    {
        super(s);
    }

    public void random(int resourceCount)
    {
        super.random();
        updatefitness();
    }

//    double calfitness()
//    {
//        return fitness;
//    }

    public double updatefitness()
    {
        return this.getFitness();
//        fitness = this.getFitness();
//        return fitness;
//        fitness = 0;
//        if (this.getMakespan() > DeadLine)
//        {
//            double PenaltyFunction = C + WEIGHT * (this.getMakespan() - DeadLine);
//            fitness = this.getCost() + PenaltyFunction;
//        }
//        else
//        {
//            fitness = this.getCost();
//        }
//        return fitness;
    }

    void mutation()
    {
        int randomTask = r.nextInt(settings.getTotalTasks());
        int randomServer = r.nextInt(settings.getTotalWorkers());
        setWorkerForTask(randomTask, settings.getWorker(randomServer));
//        updatefitness();
    }

    GAIndividual[] crossover(GAIndividual s2)
    {
        int crossoverPoint = r.nextInt(settings.getTotalTasks());
        GAIndividual[] child = new GAIndividual[2];
        child[0] = new GAIndividual(settings);
        child[1] = new GAIndividual(settings);
        for (int i = 0; i < crossoverPoint; i++)
        {
            child[0].setWorkerForTask(i, this.getWorkerForTask(i));
            child[1].setWorkerForTask(i, s2.getWorkerForTask(i));
        }
        for (int i = crossoverPoint; i < settings.getTotalTasks(); i++)
        {
            child[0].setWorkerForTask(i, s2.getWorkerForTask(i));
            child[1].setWorkerForTask(i, this.getWorkerForTask(i));
        }
        child[0].updatefitness();
        child[1].updatefitness();
        return child;
    }
}
