# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring

import magnus


class SfnState:
    state_type = ''  # Should map to one of magnus internal node types

    def __init__(self, node, graph, executor):
        self.node = node
        self.graph = graph
        self.executor = executor

    def render(self):
        raise NotImplementedError


class SuccessState(SfnState):
    state_type = 'success'

    def render(self):
        return {'Type': "Succeed"}


class FailState(SfnState):
    state_type = 'fail'

    def render(self):
        return {'Type': "Fail"}


class TaskState(SfnState):
    state_type = 'task'

    def render_catch(self):
        catch = {}
        catch['ErrorEquals'] = ["States.ALL"]

        fail_node = self.node.get_on_failure_node() or self.graph.get_fail_node().name
        catch['Next'] = fail_node
        return [catch]

    def compute_specific(self, step_definition):
        effective_node_config = self.executor.resolve_node_config(self.node)

        if effective_node_config.get('compute_as') == 'lambda':
            step_definition['Resource'] = effective_node_config.get('function_arn', None)  #  This can be generalized
        elif effective_node_config.get('compute_as') == 'sagemaker-processing':
            step_definition['Resource'] = "arn:aws:states:::sagemaker:createProcessingJob.sync"

            parameters = {}
            parameters['RoleArn'] = "${SageMakerAPIExecutionRoleArn}"
            parameters['ProcessingJobName'] = f"$$.Execution.Id_{self.node.name}"

            parameters["AppSpecification"] = {}
            parameters["AppSpecification"]["ImageUri"] = effective_node_config['image_uri']
            parameters["AppSpecification"]["ContainerEntrypoint"] = magnus.utils.get_node_execution_command(
                self.executor, self.node, over_write_run_id="$$.Execution.Id")

            parameters['ProcessingResources'] = effective_node_config['ProcessingResources']

            if 'NetworkConfig' in effective_node_config:
                parameters['NetworkConfig'] = effective_node_config['NetworkConfig']

            step_definition['Parameters'] = parameters

        else:
            raise Exception('Unsupported compute_as in the step function definition')

        return step_definition

    def render(self):
        step_definition = {}

        step_definition['Next'] = self.node.get_next_node()
        step_definition['Type'] = 'Task'
        step_definition['Catch'] = self.render_catch()

        return self.compute_specific(step_definition=step_definition)


class ParallelState(SfnState):
    state_type = 'parallel'

    def render_branches(self):
        branches = []
        for _, branch in self.node.branches.items():
            sfn_dict = {}
            sfn_dict["StartAt"] = branch.start_at

            for node in branch.nodes:
                state = None

                state_class = get_state(node.node_type)
                state = state_class(node, branch, self.executor)

                sfn_dict[node.name] = state.render()
            branches.append(sfn_dict)

        return branches

    def render(self):
        step_definition = {}

        step_definition['Next'] = self.node.get_next_node()
        step_definition['Type'] = 'Parallel'

        step_definition['Branches'] = self.render_branches()

        return step_definition


class DagState(SfnState):
    state_type = 'dag'

    def render(self):
        pass


class MapState(SfnState):
    state_type = 'map'

    def render(self):
        """
        There are some options here:
        1). A direct translation to map state, requires a pre-state which populates the parameters for SFN.
        2). Using SQS, call an async lambda which populates the jobs for the map state and a next lambda which checks
        if all the jobs are done to finish the loop.
        """
        pass


def get_state(state_type):
    for subclass in SfnState.__subclasses__():
        if subclass.state_type == state_type:
            return subclass

    raise Exception('Unidentified State')
