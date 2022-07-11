import json

from magnus.executor import BaseExecutor
from magnus.nodes import BaseNode
from magnus import exceptions
from magnus import defaults
from magnus.graph import Graph

from magnus_extension_executor_aws_sfn import states


class StateMachineExecutor(BaseExecutor):
    service_name = 'aws-sfn'

    def prepare_for_node_execution(self, node: BaseNode, map_variable: dict = None):
        """
        This method would be called prior to the node execution in the environment of the compute.

        Use this method to set up the required things for the compute.
        The most common examples might be to ensure that the appropriate run log is in place.

        NOTE: You might need to over-ride this method.
        For interactive modes, prepare_for_graph_execution takes care of a lot of set up. For orchestrated modes,
        the same work has to be done by prepare_for_node_execution.
        """
        super().prepare_for_node_execution(node=node, map_variable=map_variable)

        # Set up the run log or create it if not done previously
        try:
            # Try to get it if previous steps have created it
            run_log = self.run_log_store.get_run_log_by_id(self.run_id)
            if run_log.status in [defaults.FAIL, defaults.SUCCESS]:
                msg = (
                    f'The run_log for run_id: {self.run_id} already exists and is in {run_log.status} state.'
                    ' Make sure that this was not run before.'
                )
                raise Exception(msg)
        except exceptions.RunLogNotFoundError:
            # Create one if they are not created
            self.set_up_run_log()

        # Need to set up the step log for the node as the entry point is different
        step_log = self.run_log_store.create_step_log(node.name, node.get_step_log_name(map_variable))

        self.add_code_identities(node=node, step_log=step_log)

        step_log.step_type = node.node_type
        step_log.status = defaults.PROCESSING
        self.run_log_store.add_step_log(step_log, self.run_id)

    def execute_node(self, node: BaseNode, map_variable: dict = None, **kwargs):
        """
        This method does the actual execution of a task, as-is, success or fail node.

        NOTE: Most often, you should not be over-riding this.
        """
        super().execute_node(node, map_variable=map_variable, **kwargs)

        step_log = self.run_log_store.get_step_log(node.get_step_log_name(map_variable), self.run_id)
        if step_log.status == defaults.FAIL:
            raise Exception(f'Step {node.name} failed')

    def trigger_job(self, node: BaseNode, map_variable: dict = None, **kwargs):
        self.prepare_for_node_execution(node, map_variable=map_variable)
        self.execute_node(node=node, map_variable=map_variable, **kwargs)

    def send_return_code(self, stage='traversal'):
        """
        Convenience function used by pipeline to send return code to the caller of the cli

        Raises:
            Exception: If the pipeline execution failed
        """
        if stage != 'traversal':  # traversal does no actual execution, so return code is pointless
            run_id = self.run_id

            run_log = self.run_log_store.get_run_log_by_id(run_id=run_id, full=False)
            if run_log.status == defaults.FAIL:
                raise Exception('Pipeline execution failed')

    def execute_graph(self, dag: Graph, map_variable: dict = None, **kwargs):
        """
        Iterate through the graph and frame the bash script.

        For more complex outputs, dataclasses might be a better option.

        NOTE: This method should be over-written to write the exact specification to the compute engine.

        """
        sfn_dict = {}
        sfn_dict["StartAt"] = dag.start_at
        sfn_dict['Comment'] = dag.description
        sfn_dict['TimeoutSeconds'] = dag.max_time

        for node in self.dag.nodes:
            state = None

            state_class = states.get_state(node.node_type)
            state = state_class(node, dag, self)

            sfn_dict[node.name] = state.render()

        print(json.dumps(sfn_dict, indent=4))
