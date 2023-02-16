import logging
import shlex

from kfp import dsl
from kfp.compiler.compiler import Compiler
from kubernetes.client import V1EnvVar, V1EnvVarSource, V1SecretKeySelector

from magnus import defaults
from magnus import integration
from magnus.executor import BaseExecutor
from magnus.graph import Graph
from magnus.nodes import BaseNode
from magnus import utils

logger = logging.getLogger(defaults.NAME)

_EXECUTOR = None
_GRAPH = None


def secret_to_env_var(secret_name: str, secret_key: str, env_var_name: str) -> V1EnvVar:
    """Turns a K8s secret into an environment variable for use in a kubeflow component"""
    return V1EnvVar(
        name=env_var_name,
        value_from=V1EnvVarSource(secret_key_ref=V1SecretKeySelector(name=secret_name, key=secret_key)),
    )


class KubeFlowExecutor(BaseExecutor):

    service_name = "kfp"

    class Config(BaseExecutor.Config):
        docker_image: str
        output_file: str = 'pipeline.yaml'
        default_cpu_limit: str = "250m"
        default_memory_limit: str = "1G"
        default_cpu_request: str = ""
        default_memory_request: str = ""
        enable_caching: bool = False
        image_pull_policy: str = "IfNotPresent"
        secrets_from_k8s: dict = None  # EnvVar=SecretName:Key

    def prepare_for_graph_execution(self):
        """
        This method would be called prior to calling execute_graph.
        Perform any steps required before doing the graph execution.

        The most common implementation is to prepare a run log for the run if the run uses local interactive compute.

        But in cases of actual rendering the job specs (eg: AWS step functions, K8's) we need not do anything.
        """

        integration.validate(self, self.run_log_store)
        integration.configure_for_traversal(self, self.run_log_store)

        integration.validate(self, self.catalog_handler)
        integration.configure_for_traversal(self, self.catalog_handler)

        integration.validate(self, self.secrets_handler)
        integration.configure_for_traversal(self, self.secrets_handler)

    def prepare_for_node_execution(self):
        """
        Perform any modifications to the services prior to execution of the node.

        Args:
            node (Node): [description]
            map_variable (dict, optional): [description]. Defaults to None.
        """

        integration.validate(self, self.run_log_store)
        integration.configure_for_execution(self, self.run_log_store)

        integration.validate(self, self.catalog_handler)
        integration.configure_for_execution(self, self.catalog_handler)

        integration.validate(self, self.secrets_handler)
        integration.configure_for_execution(self, self.secrets_handler)

        self._set_up_run_log(exists_ok=True)

    def trigger_job(self, node: BaseNode, map_variable: dict = None, **kwargs):
        # TODO: This might have to be removed once core is corrected
        pass

    def execute_node(self, node: BaseNode, map_variable: dict = None, **kwargs):
        step_log = self.run_log_store.create_step_log(node.name, node._get_step_log_name(map_variable))

        self.add_code_identities(node=node, step_log=step_log)

        step_log.step_type = node.node_type
        step_log.status = defaults.PROCESSING
        self.run_log_store.add_step_log(step_log, self.run_id)

        super()._execute_node(node, map_variable=map_variable, **kwargs)

        # Implicit fail
        _, next_node_name = self._get_status_and_next_node_name(node, self.dag, map_variable=map_variable)
        next_node = self.dag.get_node_by_name(next_node_name)

        if next_node.node_type == defaults.FAIL:
            self.execute_node(next_node, map_variable=map_variable)

        step_log = self.run_log_store.get_step_log(node._get_step_log_name(map_variable), self.run_id)
        if step_log.status == defaults.FAIL:
            raise Exception(f'Step {node.name} failed')

    def create_container_op(self, working_on: BaseNode):
        command = shlex.split(utils.get_node_execution_command(self, working_on,
                                                               over_write_run_id='{{workflow.uid}}'))
        mode_config = self._resolve_node_config(working_on)

        docker_image = mode_config['docker_image']
        secrets = mode_config.get("secrets_from_k8s", {})

        operator = dsl.ContainerOp(name=working_on._command_friendly_name(), image=docker_image, command=command)

        cpu_limit = mode_config.get('cpu_limit', self.config.default_cpu_limit)
        memory_limit = mode_config.get('memory_limit', self.config.default_memory_limit)

        cpu_request = mode_config.get('cpu_request', self.config.default_cpu_request) or cpu_limit
        memory_request = mode_config.get('memory_request', self.config.default_memory_request) or memory_limit

        operator.set_memory_limit(memory_limit).set_memory_request(
            memory_request).set_cpu_request(cpu_request).set_cpu_limit(cpu_limit)

        operator.set_retry(str(working_on._get_max_attempts()))
        operator.container.set_image_pull_policy(self.config.image_pull_policy)

        for secret_env, k8_secret in secrets.items():
            try:
                secret_name, key = k8_secret.split(':')
            except Exception as _e:
                msg = (
                    "K8's secret should be of format EnvVar=SecretName:Key"
                )
                raise Exception(msg) from _e
            operator.add_env_variable(secret_to_env_var(secret_name=secret_name,
                                      secret_key=key, env_var_name=secret_env))

        return operator

    def execute_graph(self, dag: Graph, map_variable: dict = None, **kwargs):
        global _EXECUTOR, _GRAPH

        _EXECUTOR = self
        _GRAPH = dag

        Compiler().compile(convert_to_kfp, self.config.output_file)

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


@dsl.pipeline(name='magnus-dag')
def convert_to_kfp():
    """
    Just to satisfy the way KFP is designed.
    """
    dag = _GRAPH
    self = _EXECUTOR

    current_node = dag.start_at
    previous_node = None
    logger.info(f'Rendering job started at {current_node}')

    container_operators = {}
    previous_operator = None
    while True:
        working_on = dag.get_node_by_name(current_node)
        if working_on.is_composite:
            raise NotImplementedError('Composite nodes would not be implemented in Kubeflow, check Argo extension')

        if previous_node == current_node:
            raise Exception('Potentially running in a infinite loop')

        container_operator = self.create_container_op(working_on=working_on)
        if container_operator not in container_operators:
            container_operators[current_node] = container_operator

        if previous_operator:
            container_operator.after(previous_operator)

        previous_node = current_node
        previous_operator = container_operator

        fail_node_name = working_on._get_on_failure_node()

        if fail_node_name:  # Not the Fail node of the graph
            logger.warning('KFP does not support on_failure without significant workarounds! Use Argo extension instead')
            current_node = working_on._get_next_node()
            continue

        if fail_node_name:
            fail_node = dag.get_node_by_name(fail_node_name)
            fail_operator = self.create_container_op(working_on=fail_node)
            if fail_node_name not in container_operators:
                container_operators[fail_node_name] = fail_operator

        if working_on.node_type in ['success', 'fail']:
            break

        current_node = working_on._get_next_node()
