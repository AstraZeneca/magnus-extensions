import logging
import shlex

from kfp.onprem import mount_pvc
from magnus import defaults, integration, utils
from magnus.executor import BaseExecutor
from magnus.graph import Graph
from magnus.nodes import BaseNode
from ruamel.yaml import YAML

logger = logging.getLogger(defaults.NAME)

try:
    from kfp import dsl
    from kfp.compiler.compiler import Compiler
    from kubernetes.client import V1EnvVar, V1EnvVarSource, V1SecretKeySelector
except ImportError as _e:
    msg = (
        "Kubeflow Dependencies have not been installed!!"
    )
    raise Exception(msg) from _e


_EXECUTOR = None
_GRAPH = None
_PARAMETERS = None
_NODE = None


def secret_to_env_var(secret_name: str, secret_key: str, env_var_name: str) -> V1EnvVar:
    """Turns a K8s secret into an environment variable for use in a kubeflow component"""
    return V1EnvVar(
        name=env_var_name,
        value_from=V1EnvVarSource(secret_key_ref=V1SecretKeySelector(name=secret_name, key=secret_key)),
    )


def disable_cache(task):
    """Disables caching on passed task"""
    task.execution_options.caching_strategy.max_cache_staleness = "P0D"


class KubeFlowExecutor(BaseExecutor):

    service_name = "kfp"
    run_id_placeholder = dsl.RUN_ID_PLACEHOLDER
    replace_run_id = "{{workflow.parameters.run_id}}"

    class Config(BaseExecutor.Config):
        docker_image: str
        output_file: str = 'pipeline.yaml'
        cpu_limit: str = "250m"
        memory_limit: str = "1G"
        gpu_limit: int = 0
        cpu_request: str = ""
        memory_request: str = ""
        enable_caching: bool = False
        image_pull_policy: str = "Always"
        secrets_from_k8s: dict = {}  # EnvVar=SecretName:Key
        persistent_volumes: dict = {}  # volume-name:mount_path

    def __init__(self, config: dict = None):
        super().__init__(config)
        self.persistent_volumes = {}

        for i, (volume_name, mount_path) in enumerate(self.config.persistent_volumes.items()):
            self.persistent_volumes[f"executor-{i}"] = (volume_name, mount_path)

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

        integration.validate(self, self.experiment_tracker)
        integration.configure_for_traversal(self, self.experiment_tracker)

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

        integration.validate(self, self.experiment_tracker)
        integration.configure_for_execution(self, self.experiment_tracker)

        self._set_up_run_log(exists_ok=True)

    def execute_job(self, node: BaseNode):
        global _EXECUTOR, _PARAMETERS, _NODE

        _EXECUTOR = self
        _NODE = node

        compiler = Compiler()
        # set parameters as Kfp parameters
        parameters = utils.get_user_set_parameters()
        if self.parameters_file:
            parameters.update(utils.load_yaml(self.parameters_file))
        _PARAMETERS = parameters

        pipeline_params = []
        for param_key, param_value in parameters.items():
            param = dsl.PipelineParam(name=param_key, value=param_value)
            pipeline_params.append(param)

        pipeline_params.append(dsl.PipelineParam(name='run_id', value=self.run_id_placeholder))

        pipeline = compiler.create_workflow(pipeline_func=convert_to_kfp_job,
                                            pipeline_name='magnus_dag', params_list=pipeline_params)

        # compiler.compile(convert_to_kfp, self.config.output_file)
        yaml = YAML()
        with open(self.config.output_file, 'w') as f:
            yaml.dump(pipeline, f)

    def execute_node(self, node: BaseNode, map_variable: dict = None, **kwargs):
        step_log = self.run_log_store.create_step_log(node.name, node._get_step_log_name(map_variable))

        self.add_code_identities(node=node, step_log=step_log)

        step_log.step_type = node.node_type
        step_log.status = defaults.PROCESSING
        self.run_log_store.add_step_log(step_log, self.run_id)

        super()._execute_node(node, map_variable=map_variable, **kwargs)

        # Implicit fail
        if self.dag:
            # notebook or func does not have dag definitions.
            _, next_node_name = self._get_status_and_next_node_name(node, self.dag, map_variable=map_variable)
            # Terminal nodes do not have next node
            if next_node_name:
                next_node = self.dag.get_node_by_name(next_node_name)

                if next_node.node_type == defaults.FAIL:
                    self.execute_node(next_node, map_variable=map_variable)

        step_log = self.run_log_store.get_step_log(node._get_step_log_name(map_variable), self.run_id)
        if step_log.status == defaults.FAIL:
            raise Exception(f'Step {node.name} failed')

    def create_container_op(self, working_on: BaseNode, command: str):
        global _GRAPH
        command = shlex.split(command)
        mode_config = self._resolve_node_config(working_on)

        docker_image = mode_config['docker_image']
        secrets = mode_config.get("secrets_from_k8s", {})

        operator = dsl.ContainerOp(name=working_on._command_friendly_name(), image=docker_image, command=command)

        cpu_limit = mode_config.get('cpu_limit', self.config.cpu_limit)
        memory_limit = mode_config.get('memory_limit', self.config.memory_limit)

        cpu_request = mode_config.get('cpu_request', self.config.cpu_request) or cpu_limit
        memory_request = mode_config.get('memory_request', self.config.memory_request) or memory_limit

        gpu_limit = mode_config.get("gpu_limit", self.config.gpu_limit)

        operator.set_memory_limit(memory_limit).set_memory_request(
            memory_request).set_cpu_request(cpu_request).set_cpu_limit(cpu_limit)

        if gpu_limit:
            operator.set_gpu_limit(gpu_limit)

        operator.set_retry(str(working_on._get_max_attempts()))

        image_pull_policy = mode_config.get("image_pull_policy", self.config.image_pull_policy)
        operator.container.set_image_pull_policy(image_pull_policy)

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

        visited_claims = {}
        for volume_name, claim in self.persistent_volumes.items():
            claim_name, mount_path = claim

            # If the volume is already mounted, we cannot mount it again.
            if claim_name in visited_claims:
                msg = (
                    "The same persistent volume claim has already been used in the pipeline by another service"
                )
                raise Exception(msg)
            visited_claims[claim_name] = claim_name

            operator.apply(mount_pvc(pvc_name=claim_name, volume_name=volume_name, volume_mount_path=mount_path))

        if _GRAPH and _GRAPH.start_at == working_on.name:
            global _PARAMETERS
            parameters = _PARAMETERS

            for key, _ in parameters.items():
                operator.add_env_variable(V1EnvVar(name=defaults.PARAMETER_PREFIX + key,
                                          value="{{workflow.parameters." + key + "}}"))

        return operator

    def execute_graph(self, dag: Graph, map_variable: dict = None, **kwargs):
        global _EXECUTOR, _GRAPH, _PARAMETERS

        _EXECUTOR = self
        _GRAPH = dag

        compiler = Compiler()
        # set parameters as Kfp parameters
        parameters = utils.get_user_set_parameters()
        if self.parameters_file:
            parameters.update(utils.load_yaml(self.parameters_file))
        _PARAMETERS = parameters

        pipeline_params = []
        for param_key, param_value in parameters.items():
            param = dsl.PipelineParam(name=param_key, value=param_value)
            pipeline_params.append(param)

        pipeline_params.append(dsl.PipelineParam(name='run_id', value=self.run_id_placeholder))

        pipeline = compiler.create_workflow(pipeline_func=convert_to_kfp,
                                            pipeline_name='magnus_dag', params_list=pipeline_params)
        # compiler.compile(convert_to_kfp, self.config.output_file)
        yaml = YAML()
        with open(self.config.output_file, 'w') as f:
            yaml.dump(pipeline, f)

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


def convert_to_kfp_job():
    """
    The function that makes the workflow
    """
    self = _EXECUTOR
    node = _NODE
    parameters = _PARAMETERS

    command = utils.get_job_execution_command(self, node,
                                              over_write_run_id=self.replace_run_id)
    container_operator = self.create_container_op(working_on=node, command=command)

    for key, _ in parameters.items():
        container_operator.add_env_variable(V1EnvVar(name=defaults.PARAMETER_PREFIX + key,
                                                     value="{{workflow.parameters." + key + "}}"))

    # Disable caching if asked
    if not self.config.enable_caching:
        dsl.get_pipeline_conf().add_op_transformer(disable_cache)


def convert_to_kfp():
    """
    The function that makes the workflow
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

        command = utils.get_node_execution_command(self, working_on,
                                                   over_write_run_id=self.replace_run_id)
        container_operator = self.create_container_op(working_on=working_on, command=command)

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

    # Disable caching if asked
    if not self.config.enable_caching:
        dsl.get_pipeline_conf().add_op_transformer(disable_cache)
