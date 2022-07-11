import logging
import sys
from typing import List
from collections import defaultdict

from pydantic import BaseModel
import ruamel.yaml

from magnus.executor import BaseExecutor
from magnus.nodes import BaseNode
from magnus.graph import Graph
from magnus import defaults
from magnus import datastore
from magnus import exceptions
from magnus.graph import search_node_by_internal_name

from magnus_extension_argo_executor.node_types import ArgoTemplate, get_dag_templates

logger = logging.getLogger(defaults.NAME)

DEFAULT_CPU = '1'
DEFAULT_MEMORY = '2G'

# Use this pattern to nest and for even parallel nodes.
# https://github.com/argoproj/argo-workflows/blob/master/examples/dag-nested.yaml


class WorkSpec(BaseModel):
    apiVersion: str = 'argoproj.io/v1alpha1'
    kind: str = 'Workflow'
    metadata: dict = {}
    spec: dict = {'entrypoint': 'magnus-dag'}
    templates: List[ArgoTemplate] = []

    class Config:
        fields = {'spec': {'exclude': True},
                  'templates': {'exclude': True}}


class ArgoExecutor(BaseExecutor):
    """
    This mode should create an argo workflow definition.

    Example config:

    mode:
      type: argo-workflow
      config:
        image: optionally can be presented per step
        workflow-name:  # Optional generateName of workspec, defaults to magnus-dag-
        resources:
          memory:
          cpu:

    """
    service_name = 'argo-workflow'

    def __init__(self, config):
        # pylint: disable=R0914,R0913
        super().__init__(config=config)

        self.workspec = None
        self.default_input_artifacts = []  # List of dictionaries {name, path}, set archive to None
        self.default_output_artifacts = []

    def is_parallel_execution(self) -> bool:  # pylint: disable=R0201
        """
        Controls the parallelization of branches in map and parallel state.

        We do not allow parallel execution to happen within a single argo single job.

        Returns:
            bool: False as within a single job we do not allow parallelization
        """
        return False

    @property
    def default_memory(self) -> str:
        """
        Default memory of an argo job

        Returns:
            str: The default memory.
        """
        if 'resources' in self.config:
            return self.config['resources'].get('memory', DEFAULT_MEMORY)
        return DEFAULT_MEMORY

    @property
    def default_cpu(self) -> str:
        """
        The default CPU allocated for an argo job

        Returns:
            str: default CPU allocated
        """
        if 'resources' in self.config:
            return self.config['resources'].get('cpu', DEFAULT_CPU)
        return DEFAULT_CPU

    @property
    def default_image(self) -> str:
        """
        The default image to run.
        Can be None as the steps itself can have the image specified.

        Returns:
            str: The default image as specified in the compute config or None.
        """
        return self.config.get('image', None)

    @property
    def default_template(self) -> ArgoTemplate:
        """
        The default template for an Argo job.

        The default template should be used for all TASK steps.
        It has defaults for CPU, memory and the container.

        It is expected that run log, catalog uses artifacts to control the data flow.

        Returns:
            ArgoTemplate: The default argo template.
        """
        default_template = ArgoTemplate(name='default-template',
                                        resources={'requests': {
                                            'memory': self.default_memory,
                                            'cpu': self.default_cpu
                                        }})

        if self.default_image:
            default_template.image = self.default_image

        if self.default_input_artifacts:
            default_template.inputs = {'artifacts': self.default_input_artifacts}

        if self.default_output_artifacts:
            default_template.outputs = {'artifacts': self.default_output_artifacts}

        return default_template

    def prepare_for_graph_execution(self):
        """
        Should take care of integrations between run log store, catalog for rendering.

        Use self.default_input_artifacts and self.default_output_artifacts to provision run log, catalog
        """
        ...

    def prepare_for_node_execution(self, node: BaseNode, map_variable: dict = None):
        """
        If the run log is not set up, set it up.

        Perform any other service integrations as required.

        Args:
            node(Node): [description]
            map_variable(dict, optional): [description]. Defaults to None.
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
        Call super.

        Check if the next node is terminal, act accordingly.

        Args:
            node(Node): The node to execute
            map_variable(dict, optional): If the node is of a map state, map_variable is the value of the iterable.
                        Defaults to None.
        """
        super().execute_node(node, map_variable=map_variable, **kwargs)

        dag_being_run = self.dag

        if node.internal_branch_name:
            _, dag_being_run = search_node_by_internal_name(self.dag, node.internal_name)

        _, next_node_name = self.get_status_and_next_node_name(node, dag_being_run, map_variable=map_variable)
        next_node = dag_being_run.get_node_by_name(next_node_name)

        if next_node.node_type in ['success', 'fail']:
            # Implicit success and failure
            step_log = self.run_log_store.create_step_log(next_node.name, next_node.get_step_log_name(map_variable))
            self.run_log_store.add_step_log(step_log, self.run_id)
            super().execute_node(node, map_variable=map_variable, **kwargs)
            return

    def add_code_identities(self, node: BaseNode, step_log: datastore.StepLog, **kwargs):
        """
        Add code identities specific to the implementation.

        The Base class has an implementation of adding git code identities.

        Args:
            step_log(object): The step log object
            node(BaseNode): The node we are adding the step log for
        """
        super().add_code_identities(node=node, step_log=step_log)

    def trigger_job(self, node: BaseNode, map_variable: dict = None, **kwargs):
        """
        As we do not come in from execute graph, we do not implement this function.

        Args:
            node(BaseNode): The node to execute
            map_variable(str, optional): If the node if of a map state, this corresponds to the value of iterable.
                    Defaults to ''.
        """
        ...

    def send_return_code(self, stage='traversal'):
        """
        Convenience function used by pipeline to send return code to the caller of the cli

        Raises:
            Exception: If the pipeline execution failed
        """
        ...

    def _prepare_spec(self):
        """
        Prepare the base template for argo work spec.
        """
        workspec = WorkSpec()

        workspec.metadata['generateName'] = self.config.get('workflow-name', 'magnus-dag-')
        workspec.spec['entrypoint'] = 'magnus-dag'
        self.workspec = workspec

        self.workspec.templates.append(self.default_template)

    def render_spec(self):
        workspec_dict = self.workspec.dict()

        workspec_dict['specs'] = {'entrypoint': 'magnus-dag'}
        workspec_dict['spec']['templates'] = [
            x.dict(exclude_defaults=True, exclude_none=True) for x in self.workspec.templates]

        ruamel.yaml.round_trip_dump(workspec_dict, sys.stdout)

    def execute_graph(self, dag: Graph, map_variable: dict = None, **kwargs):
        self._prepare_spec()

        templates, dag_template = get_dag_templates(dag, 'magnus-dag', self)

        self.workspec.templates.extend(templates)
        self.workspec.templates.append(dag_template)

        self.render_spec()
