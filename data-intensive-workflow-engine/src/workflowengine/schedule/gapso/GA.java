package workflowengine.schedule.gapso;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.Scheduler;
import workflowengine.schedule.SchedulerSettings;


/**
 *
 * @author Dew
 */
public class GA implements Scheduler
{

    static SchedulerSettings ss;
    final static double ITERATION = 50;
    final static double ELITISM = 0.2;
    final static int POP_SIZE = 20;
    final static double MUTATE_RATE = 0.7;
//    final static double CROSSOVER_RATE = 0.5;
    List<GAIndividual> population;
    final static double DeadLine = 200;
    final static double kt = 0.3;
    final static int kr = 4;
//    static Workflow wf;
//    static ExecSite nw;
    static Random r = new Random();

    
    GAIndividual rouletWheel()
    {
        double total = 0;
        for (int j = 0; j < POP_SIZE; j++)
        {
            total = total + (1.0 / (population.get(j).getFitness()));
        }


        double sum = 0;
        double rand = r.nextDouble();
        for (int i = 0; i < POP_SIZE; i++)
        {
            sum = sum + (1.0 / (population.get(i).getFitness()) / total);
            if (rand < sum)
            {
                return population.get(i);
            }
        }
        return null;
    }

    public Schedule getSchedule(SchedulerSettings settings)
    {
        population = new ArrayList<>();
        ss = settings;
        
//        Schedule HEFTs = new HEFTScheduler().getSchedule(settings);
//        GAIndividual HEFTInd = new GAIndividual(HEFTs);
//        HEFTInd.updatefitness();
//        population.add(HEFTInd);
        for (int i = 0; i < POP_SIZE; i++)
        {
            GAIndividual ind = new GAIndividual(settings);
            ind.random(settings.getTotalWorkers());
            ind.updatefitness();
            population.add(ind);
        }
        Collections.sort(population, new ScheduleComparator());
        
        
        for (int i = 0; i < ITERATION; i++)
        {
            ArrayList<GAIndividual> newPopulation = new ArrayList<>();
            
            for (int j = 0; j < POP_SIZE * ELITISM; j++)
            {
                newPopulation.add(population.get(j));
            }
            for (int k = 0; k < (POP_SIZE - POP_SIZE * ELITISM) / 2; k++)
            {
                GAIndividual p1 = rouletWheel();
                GAIndividual p2 = rouletWheel();
                GAIndividual[] childs = p1.crossover(p2);
                if (r.nextDouble() < MUTATE_RATE)
                {
                    childs[0].mutation();
                    childs[1].mutation();
                }
                newPopulation.add(childs[0]);
                newPopulation.add(childs[1]);
            }
            newPopulation.addAll(population);
            Collections.sort(newPopulation, new ScheduleComparator());
            population = newPopulation.subList(0, POP_SIZE);
            System.out.println(population.get(0).getFitness());
        }
        return population.get(0);
    }
    
   
}
class ScheduleComparator implements Comparator<GAIndividual>
{
    public int compare(GAIndividual o1, GAIndividual o2)
    {
        if (o1.getFitness() < o2.getFitness())
        {
            return -1;
        }
        else if (o1.getFitness() == o2.getFitness())
        {
            return 0;
        }
        else
        {
            return 1;
        }
    }
}