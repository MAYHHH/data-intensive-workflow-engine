/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule.fc;

import workflowengine.schedule.Schedule;


/**
 *
 * @author orachun
 */
public class CostOptimizationFC implements FC
{
    public static final String PROP_DEADLINE = "deadline";
    public static final String PROP_CONSTANT_PENALTY = "constant_penalty";
    public static final String PROP_WEIGHTED_PENALTY = "weighted_penalty";
    
    private double CONSTANT_PENALTY = 500;
    private double WEIGHT = 100;
    private double DEADLINE = -1;

    public CostOptimizationFC()
    {
    }

    public CostOptimizationFC(double deadline, double constant, double weight)
    {
        this.CONSTANT_PENALTY = constant;
        this.DEADLINE = deadline;
        this.WEIGHT = weight;
    }
    
    @Override
    public double getFitness(Schedule sch)
    {
        double fitness = 0;
        if (DEADLINE > 0 && sch.getMakespan() > DEADLINE)
        {
            double PenaltyFunction = CONSTANT_PENALTY + WEIGHT * (sch.getMakespan() - DEADLINE);
            fitness = sch.getCost() + PenaltyFunction;
        }
        else
        {
            fitness = sch.getCost();
        }
        return fitness;
    }
    
}
