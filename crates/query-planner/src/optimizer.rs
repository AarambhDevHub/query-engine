use crate::logical_plan::LogicalPlan;
use query_core::Result;
use std::sync::Arc;

pub struct Optimizer {
    rules: Vec<Box<dyn OptimizationRule>>,
}

impl Optimizer {
    pub fn new() -> Self {
        Self {
            rules: vec![Box::new(PredicatePushdown), Box::new(ProjectionPushdown)],
        }
    }

    pub fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let mut optimized = plan.clone();

        for rule in &self.rules {
            optimized = rule.optimize(&optimized)?;
        }

        Ok(optimized)
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

trait OptimizationRule {
    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan>;
}

struct PredicatePushdown;

impl OptimizationRule for PredicatePushdown {
    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter { input, predicate } => match input.as_ref() {
                LogicalPlan::Projection {
                    input,
                    exprs,
                    schema,
                } => Ok(LogicalPlan::Projection {
                    input: Arc::new(LogicalPlan::Filter {
                        input: input.clone(),
                        predicate: predicate.clone(),
                    }),
                    exprs: exprs.clone(),
                    schema: schema.clone(),
                }),
                _ => Ok(plan.clone()),
            },
            _ => Ok(plan.clone()),
        }
    }
}

struct ProjectionPushdown;

impl OptimizationRule for ProjectionPushdown {
    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        // Simplified projection pushdown
        Ok(plan.clone())
    }
}
