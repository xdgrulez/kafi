```
agg(is)

lis = LiftedStreamIntroduction(is) - Lift1 - UnaryOperator
    * input_stream_handle
    * output_stream_handle
      
g = GroupByThenAgg(lis.output_handle()) - UnaryOperator
  * input_stream_handle
    
    self.integrated_stream = Integrate(input_stream_handle)
      Integrate - UnaryOperator
        * input_stream_handle
          self.integration_stream = LiftedGroupAdd(input_stream_handle, None)
            LiftedGroupAdd - Lift2 - BinaryOperator
              * input_stream_handle_a
              * input_stream_handle_b
              * output_stream_handle
          self.delayed_stream = Delay(self.integration_stream.output_handle())
            Delay - UnaryOperator
              * input_stream_handle
              * output_stream_handle
          self.integration_stream.set_input_b(self.delayed_stream.output_handle())
          self.output_stream_handle = self.integration_stream.output_handle()
        * output_stream_handle
    
    self.lifted_integrated_stream = LiftedIntegrate(self.integrated_stream.output_handle())
      LiftedIntegrate - Lift1 - UnaryOperator
        * input_stream_handle
          Integrate - UnaryOperator
            * input_stream_handle
              self.integration_stream = LiftedGroupAdd(input_stream_handle, None)
                LiftedGroupAdd - Lift2 - BinaryOperator
                  * input_stream_handle_a
                  * input_stream_handle_b
                  * output_stream_handle
              self.delayed_stream = Delay(self.integration_stream.output_handle())
                Delay - UnaryOperator
                  * input_stream_handle
                  * output_stream_handle
              self.integration_stream.set_input_b(self.delayed_stream.output_handle())
              self.output_stream_handle = self.integration_stream.output_handle()
          * output_stream_handle
        * output_stream_handle

    self.lifted_lifted_aggregate = LiftedLiftedAggregate(self.lift_integrated_stream.output_handle(), self.by, self.aggregation)
      LiftedLiftedAggregate - Lift1 - UnaryOperator
        * input_stream_handle
          LiftedIntegrate - Lift1 - UnaryOperator
            * input_stream_handle
              Integrate - UnaryOperator
                * input_stream_handle
                  self.integration_stream = LiftedGroupAdd(input_stream_handle, None)
                    LiftedGroupAdd - Lift2 - BinaryOperator
                      * input_stream_handle_a
                      * input_stream_handle_b
                      * output_stream_handle
                  self.delayed_stream = Delay(self.integration_stream.output_handle())
                    Delay - UnaryOperator
                      * input_stream_handle
                      * output_stream_handle
                  self.integration_stream.set_input_b(self.delayed_stream.output_handle())
                  self.output_stream_handle = self.integration_stream.output_handle()
              * output_stream_handle
            * output_stream_handle
        * output_stream_handle

  self.output_stream_handle = self.lifted_lifted_aggregate.output_handle()

  * output_stream_handle

s = LiftedStreamElimination(g.output_handle())
  LiftedStreamElimination - Lift1 - UnaryOperator
    * input_stream_handle
    * output_stream_handle

d = Differentiate(s.output_handle())
  Differentiate - UnaryOperator
    * input_stream_handle
      
      self.delayed_stream = Delay(input_stream_handle)
         Delay - UnaryOperator
           * input_stream_handle
           * output_stream_handle
      
      self.delayed_negated_stream = LiftedGroupNegate(self.delayed_stream.output_handle())
        LiftedGroupNegate - Lift1 - UnaryOperator
           * input_stream_handle
           * output_stream_handle
      
      self.differentiation_stream = LiftedGroupAdd(input_stream_handle, self.delayed_negated_stream.output_handle())

      self.output_stream_handle = self.differentiation_stream.output_handle()
    
    * output_stream_handle
  
os = d.output_handle()
```
